use anyhow::Result;
use std::{fs::File, io::prelude::*};

use crate::varint;

#[derive(Debug, PartialEq, Eq)]
pub enum PageKind {
    InteriorIndex,
    LeafIndex,
    InteriorTable,
    LeafTable,
}

impl<'page> PageKind {
    pub fn is_interior(&self) -> bool {
        matches!(self, Self::InteriorIndex | Self::InteriorTable)
    }

    pub fn is_leaf(&self) -> bool {
        matches!(self, Self::LeafIndex | Self::LeafTable)
    }

    pub fn read_cell(&self, data: &'page [u8]) -> Cell<'page> {
        match self {
            PageKind::InteriorIndex => Cell::read_interior_index(data),
            PageKind::LeafIndex => Cell::read_leaf_index(data),
            PageKind::InteriorTable => Cell::read_interior_table(data),
            PageKind::LeafTable => Cell::read_leaf_table(data),
        }
    }
}

impl TryFrom<u8> for PageKind {
    type Error = anyhow::Error;

    fn try_from(value: u8) -> std::result::Result<Self, Self::Error> {
        match value {
            0x02 => Ok(Self::InteriorIndex),
            0x05 => Ok(Self::InteriorTable),
            0x0a => Ok(Self::LeafIndex),
            0x0d => Ok(Self::LeafTable),
            _ => Err(anyhow::anyhow!("Invalid page kind")),
        }
    }
}

#[derive(Debug)]
pub enum Cell<'page> {
    InteriorIndex {
        left_child_page: u32,
        size: u64,
        payload: &'page [u8],
        overflow_page: u32,
    },
    LeafIndex {
        size: u64,
        payload: &'page [u8],
        overflow_page: u32,
    },
    InteriorTable {
        left_child_page: u32,
        key: u64,
    },
    LeafTable {
        size: u64,
        rowid: u64,
        payload: &'page [u8],
        overflow_page: u32,
    },
}

impl<'page> Cell<'page> {
    fn read_interior_index(data: &'page [u8]) -> Cell {
        let left_child_page = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);

        let mut cursor = 4;
        let (size, offset) = varint::read(&data[cursor..]);
        let size = size as u64;
        cursor += offset;

        let (overflow_page, end) = if size > data[cursor..].len() as u64 {
            let end = data.len() - 4;
            (
                u32::from_be_bytes([data[end], data[end + 1], data[end + 2], data[end + 3]]),
                end,
            )
        } else {
            (0, cursor + size as usize)
        };

        Cell::InteriorIndex {
            left_child_page: left_child_page as u32,
            size,
            payload: &data[cursor..end],
            overflow_page,
        }
    }

    fn read_leaf_index(data: &'page [u8]) -> Cell {
        let mut cursor = 0;
        let (size, offset) = varint::read(&data[..]);
        let size = size as u64;
        cursor += offset;

        let (overflow_page, end) = if size > data[cursor..].len() as u64 {
            let end = data.len() - 4;
            (
                u32::from_be_bytes([data[end], data[end + 1], data[end + 2], data[end + 3]]),
                end,
            )
        } else {
            (0, cursor + size as usize)
        };

        Cell::LeafIndex {
            size,
            payload: &data[cursor..end],
            overflow_page,
        }
    }

    fn read_interior_table(data: &'page [u8]) -> Cell {
        let left_child_page = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
        let (key, _) = varint::read(&data[4..]);

        Cell::InteriorTable {
            left_child_page: left_child_page as u32,
            key: key as u64,
        }
    }

    fn read_leaf_table(data: &'page [u8]) -> Cell {
        let mut cursor = 0;
        let (size, offset) = varint::read(data);
        let size = size as u64;
        cursor += offset;

        let (rowid, offset) = varint::read(&data[cursor..]);
        cursor += offset;

        let (overflow_page, end) = if size > data[cursor..].len() as u64 {
            let end = data.len() - 4;
            (
                u32::from_be_bytes([data[end], data[end + 1], data[end + 2], data[end + 3]]),
                end,
            )
        } else {
            (0, cursor + size as usize)
        };

        Cell::LeafTable {
            size,
            rowid: rowid as u64,
            payload: &data[cursor..end],
            overflow_page,
        }
    }
}

#[derive(Debug)]
pub struct PageHeader {
    pub kind: PageKind,
    pub first_freeblock_start: u16,
    pub number_of_cells: u16,
    pub content_start_offset: u16,
    pub fragment_free_bytes: u8,
    pub right_child_page_number: u32,
}

#[derive(Debug)]
pub struct Page {
    pub header: PageHeader,
    pub cell_pointers: Vec<u16>,
    pub data: Vec<u8>,
}

impl Page {
    pub fn read(file: &mut File, page_size: u16) -> Result<Self> {
        Self::read_with_offset(file, page_size, 0)
    }

    pub fn read_with_offset(file: &mut File, page_size: u16, offset: u16) -> Result<Self> {
        let mut page = vec![0; page_size as usize];
        file.read_exact(&mut page)?;

        let kind = PageKind::try_from(u8::from_be_bytes([page[0]]))?;
        let first_freeblock_start = u16::from_be_bytes([page[1], page[2]]);
        let number_of_cells = u16::from_be_bytes([page[3], page[4]]);
        let content_start_offset = u16::from_be_bytes([page[5], page[6]]);
        let fragment_free_bytes = page[7];
        let (header_size, right_child_page_number) = if kind.is_interior() {
            (
                12,
                u32::from_be_bytes([page[8], page[9], page[10], page[11]]),
            )
        } else {
            (8, 0)
        };

        let header = PageHeader {
            kind,
            first_freeblock_start,
            number_of_cells,
            content_start_offset,
            fragment_free_bytes,
            right_child_page_number,
        };

        let cell_pointers: Vec<u16> = page[header_size..]
            .chunks_exact(2)
            .take(header.number_of_cells.into())
            .map(|chunk| u16::from_be_bytes([chunk[0], chunk[1]]) - offset as u16)
            .collect();

        Ok(Self {
            header,
            cell_pointers,
            data: page,
        })
    }

    pub fn cells(&self) -> impl Iterator<Item = Cell> {
        self.cell_pointers
            .iter()
            .map(move |pointer| self.header.kind.read_cell(&self.data[*pointer as usize..]))
    }
}
