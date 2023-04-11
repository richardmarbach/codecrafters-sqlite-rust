use std::fs::File;
use std::io::{prelude::*, SeekFrom};

use anyhow::{bail, Result};

use crate::page::{Cell, Page};
use crate::record::Record;
use crate::sql;
use crate::sqlite_schema::SQLiteSchema;

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
pub struct Database {
    pub header: DatabaseHeader,
    pub file: File,
    pub schema: SQLiteSchema,
}

impl Database {
    pub fn open(path: &str) -> Result<Self> {
        let mut file = File::open(path)?;
        let header = DatabaseHeader::read(&mut file)?;

        let page = Page::read_with_offset(&mut file, header.page_size - 100, 100)?;
        let schema = SQLiteSchema::read(page)?;

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

        let (_, definition) = sql::parse_creation(schema_definition.sql.as_bytes())
            .map_err(|_e| anyhow::anyhow!("Failed to parse table definition"))?;

        let page = self.get_page(schema_definition.rootpage - 1)?;

        let select_fields = sql_statement
            .fields
            .iter()
            .map(|sql_field| definition.find_field(sql_field).expect("Fields not found"))
            .map(|(pos, _field)| pos)
            .collect::<Vec<_>>();

        match page.header.kind {
            crate::page::PageKind::InteriorIndex => unimplemented!(),
            crate::page::PageKind::LeafIndex => unimplemented!(),
            crate::page::PageKind::InteriorTable => self.follow_references(
                &page,
                &definition,
                &select_fields,
                &sql_statement.where_clause,
                out,
            ),
            crate::page::PageKind::LeafTable => self.write_results(
                &page,
                &definition,
                &select_fields,
                &sql_statement.where_clause,
                out,
            ),
        }
    }

    fn follow_references(
        &mut self,
        page: &Page,
        definition: &sql::CreateTableStatement,
        select_fields: &[usize],
        filter: &Option<sql::WhereClause>,
        out: &mut impl std::io::Write,
    ) -> Result<()> {
        for cell in page.cells() {
            let Cell::InteriorTable { left_child_page, .. } = cell else {
                bail!("Unsupported cell type");
            };
            let page = self.get_page(left_child_page - 1)?;
            match page.header.kind {
                crate::page::PageKind::InteriorTable => {
                    self.follow_references(&page, &definition, &select_fields, &filter, out)?;
                }
                crate::page::PageKind::LeafTable => {
                    self.write_results(&page, &definition, &select_fields, &filter, out)?;
                }

                _ => bail!("Unsupported page type"),
            }
        }

        Ok(())
    }

    fn write_results(
        &self,
        page: &Page,
        definition: &sql::CreateTableStatement,
        select_fields: &[usize],
        filter: &Option<sql::WhereClause>,
        out: &mut impl std::io::Write,
    ) -> Result<()> {
        let records = page
            .cells()
            .map(|cell| match cell {
                Cell::LeafTable { payload, .. } => Ok(Record::read(payload)),
                _ => bail!("Unsupported cell type"),
            })
            .filter(|record| {
                let Ok(record) = record else { return true; };
                let Some(ref filter) = filter else {
                                return true;
                            };

                let (pos, _field) = definition
                    .find_field(&filter.field)
                    .expect("Field not found");

                format!("{}", record.values[pos]) == filter.value
            })
            .collect::<Result<Vec<Record>>>()?;

        for record in records {
            let values = select_fields
                .iter()
                .map(|i| format!("{}", record.values[*i]))
                .collect::<Vec<_>>()
                .join("|");
            write!(out, "{}\n", values)?;
        }
        Ok(())
    }
}
