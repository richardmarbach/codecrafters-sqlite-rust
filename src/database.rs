use std::fs::File;
use std::io::{prelude::*, SeekFrom};

use anyhow::{bail, Result};
use itertools::Itertools;

use crate::page::{Cell, Page};
use crate::record::{ColumnValue, Record};
use crate::sql::{self, SelectFields};
use crate::sqlite_schema::{Index, SchemaStore, Table};

#[derive(Debug)]
pub struct DatabaseHeader {
    pub page_size: u16,
}

const MAGIC_HEADER: [u8; 16] = *b"SQLite format 3\0";
impl DatabaseHeader {
    pub fn read(file: &mut File) -> Result<Self> {
        let mut header = [0; 100];
        file.read_exact(&mut header)?;

        if &header[0..16] != MAGIC_HEADER {
            return Err(anyhow::anyhow!("Invalid database file"));
        }

        Ok(Self {
            page_size: u16::from_be_bytes([header[16], header[17]]),
        })
    }
}

#[derive(Debug)]
pub struct Query<'query> {
    pub table: &'query Table,
    pub select_fields: Vec<(usize, bool)>,
    pub filter: Option<&'query sql::WhereClause>,
}

impl<'query> Query<'query> {
    pub fn new(table: &'query Table, sql_statement: &'query SelectFields) -> Self {
        let select_fields = sql_statement
            .fields
            .iter()
            .map(|sql_field| table.find_column(sql_field).expect("Fields not found"))
            .map(|(pos, field)| (pos, field.is_primary_key))
            .collect::<Vec<_>>();
        Self {
            table,
            select_fields,
            filter: sql_statement.where_clause.as_ref(),
        }
    }
}

#[derive(Debug)]
pub struct IndexQuery<'query> {
    pub table: &'query Table,
    pub select_fields: Vec<(usize, bool)>,
    pub filter: &'query sql::WhereClause,
    pub index: &'query Index,
    pub index_field: usize,
}

impl<'query> IndexQuery<'query> {
    pub fn new(
        table: &'query Table,
        sql_statement: &'query SelectFields,
        index: &'query Index,
    ) -> Self {
        let select_fields = sql_statement
            .fields
            .iter()
            .map(|sql_field| table.find_column(sql_field).expect("Fields not found"))
            .map(|(pos, field)| (pos, field.is_primary_key))
            .collect::<Vec<_>>();

        let index_field = index
            .find_column(&sql_statement.where_clause.as_ref().unwrap().field)
            .unwrap()
            .0;
        Self {
            table,
            select_fields,
            filter: &sql_statement.where_clause.as_ref().unwrap(),
            index,
            index_field,
        }
    }
}

#[derive(Debug)]
pub struct Database {
    pub header: DatabaseHeader,
    pub file: File,
    pub schema: SchemaStore,
}

impl Database {
    pub fn open(path: &str) -> Result<Self> {
        let mut file = File::open(path)?;
        let header = DatabaseHeader::read(&mut file)?;

        let page = Page::read_with_offset(&mut file, header.page_size - 100, 100)?;
        let schema = SchemaStore::read(page)?;

        Ok(Self {
            header,
            file,
            schema,
        })
    }

    pub fn get_page(&mut self, number: u32) -> Result<Page> {
        self.file.seek(SeekFrom::Start(
            number as u64 * self.header.page_size as u64,
        ))?;

        Page::read(&mut self.file, self.header.page_size)
    }

    pub fn select_fields(
        &mut self,
        sql_statement: &sql::SelectFields,
        out: &mut impl std::io::Write,
    ) -> Result<()> {
        let schema_definition = self
            .schema
            .find_table(&sql_statement.table)
            .ok_or(anyhow::anyhow!("Table not found: {}", &sql_statement.table))?
            .clone();

        if let Some(index) = schema_definition.find_applicable_index(&sql_statement.where_clause) {
            let query = IndexQuery::new(&schema_definition, sql_statement, index);
            let page = self.get_page(index.rootpage - 1)?;

            let mut results = Vec::with_capacity(self.header.page_size as usize);
            self.read_index(&page, &query, &mut results)?;
            results.sort_unstable();

            let query = Query::new(&schema_definition, sql_statement);
            let page = self.get_page(schema_definition.rootpage - 1)?;
            self.read_ids_from_table(&page, &query, &results, out)?;

            return Ok(());
        }

        let query = Query::new(&schema_definition, sql_statement);
        let page = self.get_page(schema_definition.rootpage - 1)?;
        self.read_table(&page, &query, out)
    }

    fn read_index(
        &mut self,
        page: &Page,
        query: &IndexQuery,
        results: &mut Vec<i64>,
    ) -> Result<()> {
        match page.header.kind {
            crate::page::PageKind::InteriorIndex => {
                self.read_interior_index(&page, &query, results)
            }
            crate::page::PageKind::LeafIndex => self.read_leaf_index(&page, &query, results),
            crate::page::PageKind::InteriorTable | crate::page::PageKind::LeafTable => {
                bail!("Malformed index: index contains table pages")
            }
        }
    }

    fn read_interior_index(
        &mut self,
        page: &Page,
        query: &IndexQuery,
        results: &mut Vec<i64>,
    ) -> Result<()> {
        for cell in page.cells() {
            let Cell::InteriorIndex { left_child_page, payload, .. } = cell else {
                bail!("Unsupported cell type");
            };
            let record = Record::read(0, payload);

            let ColumnValue::Text(value) = record.values[query.index_field]  else {
                let page = self.get_page(left_child_page - 1)?;
                self.read_index(&page, query, results)?;
                continue;
            };

            if query.filter.value.as_bytes() == value {
                let id = record.values.last().expect("index must have id value");
                if id.is_number() {
                    let id: i64 = id.clone().into();
                    results.push(id);
                } else {
                    return Err(anyhow::anyhow!("Id was not a number"));
                }
            }

            // if query.filter.value.as_bytes() > value {
            //     continue;
            // }

            let page = self.get_page(left_child_page - 1)?;
            self.read_index(&page, query, results)?;
        }

        if let Some(number) = page.header.right_child_page_number {
            let page = self.get_page(number - 1)?;
            self.read_index(&page, query, results)?;
        }
        Ok(())
    }

    fn read_leaf_index(
        &mut self,
        page: &Page,
        query: &IndexQuery,
        results: &mut Vec<i64>,
    ) -> Result<()> {
        let ids = page
            .cells()
            .map(|cell| match cell {
                Cell::LeafIndex { payload, .. } => Ok(Record::read(0, payload)),
                _ => bail!("Unsupported cell type"),
            })
            .filter(|record| {
                let Ok(record) = record else { return true; };
                format!("{}", record.values[query.index_field]) == query.filter.value
            })
            .map_ok(|record| {
                let id = record.values.last().expect("index must have id value");
                if id.is_number() {
                    let id: i64 = id.clone().into();
                    Ok(id)
                } else {
                    Err(anyhow::anyhow!("Id was not a number"))
                }
            });

        for id in ids {
            let id = id??;
            results.push(id);
        }

        Ok(())
    }

    fn read_ids_from_table(
        &mut self,
        page: &Page,
        query: &Query,
        ids: &[i64],
        out: &mut impl std::io::Write,
    ) -> Result<()> {
        match page.header.kind {
            crate::page::PageKind::InteriorTable => {
                self.read_ids_from_interior_table(&page, &query, ids, out)
            }
            crate::page::PageKind::LeafTable => {
                self.read_ids_from_leaf_table(&page, &query, ids, out)
            }
            crate::page::PageKind::InteriorIndex | crate::page::PageKind::LeafIndex => {
                bail!("Malformed table: table contains index pages")
            }
        }
    }
    fn read_ids_from_interior_table(
        &mut self,
        page: &Page,
        query: &Query,
        ids: &[i64],
        out: &mut impl std::io::Write,
    ) -> Result<()> {
        let mut ids = ids;
        for cell in page.cells() {
            let Cell::InteriorTable { left_child_page, key } = cell else {
                bail!("Unsupported cell type");
            };

            let split_at = ids.split_at(ids.partition_point(|id| *id < key as i64));
            let left_ids = split_at.0; // Ids to the left
            ids = split_at.1; // Ids to the right

            if !left_ids.is_empty() {
                let page = self.get_page(left_child_page - 1)?;
                self.read_ids_from_table(&page, query, left_ids, out)?;
            }
        }

        // No more ids to the right. We're done.
        if ids.len() == 0 {
            return Ok(());
        }

        if let Some(number) = page.header.right_child_page_number {
            let page = self.get_page(number - 1)?;
            self.read_ids_from_table(&page, query, ids, out)?;
        }
        Ok(())
    }

    fn read_ids_from_leaf_table(
        &self,
        page: &Page,
        query: &Query,
        ids: &[i64],
        out: &mut impl std::io::Write,
    ) -> Result<()> {
        let records = page
            .cells()
            .map(|cell| match cell {
                Cell::LeafTable { payload, rowid, .. } => Ok(Record::read(rowid, payload)),
                _ => bail!("Unsupported cell type"),
            })
            .filter(|record| {
                let Ok(record) = record else { return true; };
                ids.binary_search(&record.rowid).is_ok()
            })
            .collect::<Result<Vec<Record>>>()?;

        for record in records {
            let values = query
                .select_fields
                .iter()
                .map(|(i, is_primary_key)| {
                    if *is_primary_key {
                        format!("{}", record.rowid)
                    } else {
                        format!("{}", record.values[*i])
                    }
                })
                .collect::<Vec<_>>()
                .join("|");
            write!(out, "{}\n", values)?;
        }
        Ok(())
    }

    fn read_table(
        &mut self,
        page: &Page,
        query: &Query,
        out: &mut impl std::io::Write,
    ) -> Result<()> {
        match page.header.kind {
            crate::page::PageKind::InteriorTable => self.read_interior_table(&page, &query, out),
            crate::page::PageKind::LeafTable => self.read_leaf_table(&page, &query, out),
            crate::page::PageKind::InteriorIndex | crate::page::PageKind::LeafIndex => {
                bail!("Malformed table: table contains index pages")
            }
        }
    }

    fn read_interior_table(
        &mut self,
        page: &Page,
        query: &Query,
        out: &mut impl std::io::Write,
    ) -> Result<()> {
        for cell in page.cells() {
            let Cell::InteriorTable { left_child_page, .. } = cell else {
                bail!("Unsupported cell type");
            };

            let page = self.get_page(left_child_page - 1)?;
            self.read_table(&page, query, out)?;
        }

        if let Some(number) = page.header.right_child_page_number {
            let page = self.get_page(number - 1)?;
            self.read_table(&page, query, out)?;
        }
        Ok(())
    }

    fn read_leaf_table(
        &self,
        page: &Page,
        query: &Query,
        out: &mut impl std::io::Write,
    ) -> Result<()> {
        let records = page
            .cells()
            .map(|cell| match cell {
                Cell::LeafTable { payload, rowid, .. } => Ok(Record::read(rowid, payload)),
                _ => bail!("Unsupported cell type"),
            })
            .filter(|record| {
                let Ok(record) = record else { return true; };
                let Some(ref filter) = query.filter else {
                                return true;
                            };

                let (pos, _field) = query
                    .table
                    .find_column(&filter.field)
                    .expect("Field not found");

                format!("{}", record.values[pos]) == filter.value
            })
            .collect::<Result<Vec<Record>>>()?;

        for record in records {
            let values = query
                .select_fields
                .iter()
                .map(|(i, is_primary_key)| {
                    if *is_primary_key {
                        format!("{}", record.rowid)
                    } else {
                        format!("{}", record.values[*i])
                    }
                })
                .collect::<Vec<_>>()
                .join("|");
            write!(out, "{}\n", values)?;
        }
        Ok(())
    }
}
