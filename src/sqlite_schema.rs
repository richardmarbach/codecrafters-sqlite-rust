use crate::{
    page::{Cell, Page},
    record::{ColumnValue, Record},
};
use anyhow::Result;

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

    pub fn user_tables(&self) -> impl Iterator<Item = &SQLiteSchemaRow> {
        self.rows
            .iter()
            .filter(|row| row.kind == "table")
            .filter(|row| !row.name.starts_with("sqlite_"))
    }

    pub fn find_table(&self, table_name: &str) -> Option<&SQLiteSchemaRow> {
        self.user_tables().find(|row| row.name == table_name)
    }
}

#[derive(Debug, Clone)]
pub struct SQLiteSchemaRow {
    pub rowid: u64,
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
            let record = Record::read(payload, 5);

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
