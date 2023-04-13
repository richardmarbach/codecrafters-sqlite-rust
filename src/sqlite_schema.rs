use std::collections::HashMap;

use crate::{
    page::{Cell, Page},
    record::{ColumnValue, Record},
    sql,
};
use anyhow::Result;

#[derive(Debug)]
pub struct SchemaStore {
    pub tables: HashMap<String, Table>,
}

impl SchemaStore {
    pub fn read(page: Page) -> Result<Self> {
        let schema_table = SQLiteSchema::read(page)?;
        let mut tables: HashMap<String, Table> = HashMap::new();

        for row in schema_table.rows.iter() {
            let (_, sql) = sql::parse_create(row.sql.as_bytes())
                .map_err(|_e| anyhow::anyhow!("Failed to parse table definition"))?;

            if let sql::SQLCommand::CreateTable(t) = sql {
                let table = Table {
                    name: t.table,
                    columns: t.fields.iter().map(|f| Column::from(f)).collect(),
                    indexes: vec![],
                    rootpage: row.rootpage,
                };
                tables.insert(table.name.clone(), table);
            }
        }

        // Loop twice even if a little inefficient. This simply insures that all the tables are
        // created before the indexes are created.
        // Since the amount of tables in a typical database is small, this isn't a problem.
        // (also this is an exercise in learning rust, not a production ready database)
        for row in schema_table.rows.iter() {
            let (_, sql) = sql::parse_create(row.sql.as_bytes())
                .map_err(|_e| anyhow::anyhow!("Failed to parse table definition"))?;

            if let sql::SQLCommand::CreateIndex(i) = sql {
                let index = Index {
                    name: i.name,
                    columns: i.fields,
                    table_name: i.table,
                    rootpage: row.rootpage,
                };
                tables
                    .get_mut(&index.table_name)
                    .expect("Index without table")
                    .indexes
                    .push(index);
            };
        }

        Ok(Self { tables })
    }

    pub fn user_tables(&self) -> impl Iterator<Item = &Table> {
        self.tables
            .values()
            .filter(|table| !table.name.starts_with("sqlite_"))
    }

    pub fn find_table(&self, table_name: &str) -> Option<&Table> {
        self.user_tables().find(|table| table.name == table_name)
    }
}

impl Default for SchemaStore {
    fn default() -> Self {
        Self {
            tables: HashMap::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
    pub indexes: Vec<Index>,
    pub rootpage: u32,
}

impl Table {
    pub fn find_column(&self, column_name: &str) -> Option<(usize, &Column)> {
        self.columns
            .iter()
            .enumerate()
            .find(|(_, column)| column.name == column_name)
    }
}

impl From<Index> for Table {
    fn from(index: Index) -> Self {
        Self {
            name: index.table_name.clone(),
            columns: vec![],
            indexes: vec![index],
            rootpage: 0,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    pub is_primary_key: bool,
}

impl From<&sql::Field> for Column {
    fn from(field: &sql::Field) -> Self {
        Self {
            name: field.name.clone(),
            is_primary_key: field.is_primary_key,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Index {
    pub name: String,
    pub columns: Vec<String>,
    pub table_name: String,
    pub rootpage: u32,
}

#[derive(Debug)]
pub struct SQLiteSchema {
    pub rows: Vec<SQLiteSchemaRow>,
}

impl SQLiteSchema {
    pub fn read(page: Page) -> Result<Self> {
        let rows: Vec<SQLiteSchemaRow> = page
            .cells()
            .map(|cell| SQLiteSchemaRow::try_from(cell))
            .collect::<Result<_>>()?;

        Ok(Self { rows })
    }
}

#[derive(Debug, Clone)]
pub struct SQLiteSchemaRow {
    pub rowid: i64,
    pub kind: String,
    pub name: String,
    pub tbl_name: String,
    pub rootpage: u32,
    pub sql: String,
}

impl<'page> TryFrom<Cell<'page>> for SQLiteSchemaRow {
    type Error = anyhow::Error;

    fn try_from(cell: Cell) -> std::result::Result<Self, Self::Error> {
        if let Cell::LeafTable {
            size: _,
            rowid,
            payload,
            overflow_page: _,
        } = cell
        {
            let record = Record::read(rowid, payload);

            let mut values = record.values.into_iter();
            let kind = values
                .next()
                .and_then(|v| match v {
                    ColumnValue::Text(text) => Some(String::from_utf8_lossy(text).into()),
                    _ => None,
                })
                .map_or_else(|| Err(anyhow::anyhow!("Invalid schema kind")), Ok)?;

            let name = values
                .next()
                .and_then(|v| match v {
                    ColumnValue::Text(text) => Some(String::from_utf8_lossy(text).into()),
                    _ => None,
                })
                .map_or_else(|| Err(anyhow::anyhow!("Invalid schema name")), Ok)?;

            let tbl_name = values
                .next()
                .and_then(|v| match v {
                    ColumnValue::Text(text) => Some(String::from_utf8_lossy(text).into()),
                    _ => None,
                })
                .map_or_else(|| Err(anyhow::anyhow!("Invalid schema table name")), Ok)?;

            let rootpage = values
                .next()
                .and_then(|v| match v {
                    ColumnValue::I8(i) => Some(i as u32),
                    _ => None,
                })
                .map_or_else(|| Err(anyhow::anyhow!("Invalid schema root page")), Ok)?;

            let sql = values
                .next()
                .and_then(|v| match v {
                    ColumnValue::Text(text) => Some(String::from_utf8_lossy(text).into()),
                    _ => None,
                })
                .map_or_else(|| Err(anyhow::anyhow!("Invalid schema SQL")), Ok)?;

            Ok(SQLiteSchemaRow {
                rowid,
                kind,
                name,
                tbl_name,
                rootpage,
                sql,
            })
        } else {
            Err(anyhow::anyhow!("Invalid cell kind"))
        }
    }
}
