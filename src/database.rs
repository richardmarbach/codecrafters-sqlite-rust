use std::fs::File;
use std::io::prelude::*;

use anyhow::Result;

use crate::page::Page;
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
}
