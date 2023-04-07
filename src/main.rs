use anyhow::{bail, Result};
use std::fs::File;
use std::io::{prelude::*, SeekFrom};

#[derive(Debug)]
enum ColumnType {
    Null,
    I8,
    I16,
    I24,
    I32,
    I48,
    I64,
    F64,
    Zero,
    One,
    Blob(usize),
    Text(usize),
}

impl From<u64> for ColumnType {
    fn from(value: u64) -> Self {
        match value {
            0 => Self::Null,
            1 => Self::I8,
            2 => Self::I16,
            3 => Self::I24,
            4 => Self::I32,
            5 => Self::I48,
            6 => Self::I64,
            7 => Self::F64,
            8 => Self::Zero,
            9 => Self::One,
            n if n > 12 && n % 2 == 0 => Self::Blob((n as usize - 12) / 2),
            n if n > 13 && n % 2 == 1 => Self::Text((n as usize - 13) / 2),
            _ => unreachable!(),
        }
    }
}

#[derive(Debug)]
enum ColumnValue {
    Null,
    I8(i64),
    I16(i64),
    I24(i64),
    I32(i64),
    I48(i64),
    I64(i64),
    F64(f64),
    Zero,
    One,
    Blob(Vec<u8>),
    Text(Vec<u8>),
}

#[derive(Debug)]
struct Record {
    values: Vec<ColumnValue>,
}

impl Record {
    fn read(file: &mut File, column_count: usize) -> Result<Self> {
        let _total_bytes = read_var_int(file)?;

        let mut columns = Vec::with_capacity(column_count);
        for _ in 0..column_count {
            columns.push(ColumnType::from(read_var_int(file)? as u64));
        }

        let read_n_bytes_as_i64 = |file: &mut File, n: usize| -> Result<i64> {
            let mut bytes = [0; 8];
            file.read_exact(&mut bytes[..n])?;
            Ok(i64::from_be_bytes(bytes))
        };

        let mut values = Vec::with_capacity(column_count);
        for column in columns.iter() {
            let value = match column {
                ColumnType::Null => ColumnValue::Null,
                ColumnType::I8 => ColumnValue::I8(read_n_bytes_as_i64(file, 1)?),
                ColumnType::I16 => ColumnValue::I16(read_n_bytes_as_i64(file, 2)?),
                ColumnType::I24 => ColumnValue::I24(read_n_bytes_as_i64(file, 3)?),
                ColumnType::I32 => ColumnValue::I32(read_n_bytes_as_i64(file, 4)?),
                ColumnType::I48 => ColumnValue::I48(read_n_bytes_as_i64(file, 6)?),
                ColumnType::I64 => ColumnValue::I64(read_n_bytes_as_i64(file, 8)?),
                ColumnType::F64 => {
                    let mut bytes = [0; 8];
                    file.read_exact(&mut bytes)?;
                    ColumnValue::F64(f64::from_be_bytes(bytes))
                }
                ColumnType::Zero => ColumnValue::Zero,
                ColumnType::One => ColumnValue::One,
                ColumnType::Blob(size) => {
                    let mut contents = vec![0; *size];
                    file.read_exact(&mut contents)?;
                    ColumnValue::Blob(contents)
                }
                ColumnType::Text(size) => {
                    let mut content = vec![0; *size];
                    file.read_exact(&mut content)?;
                    ColumnValue::Text(content)
                }
            };
            values.push(value);
        }
        Ok(Record { values })
    }
}

struct Header {
    page_size: u16,
}

impl Header {
    fn read(file: &mut File) -> Result<Self> {
        let mut header = [0; 100];
        file.read_exact(&mut header)?;

        Ok(Self {
            page_size: u16::from_be_bytes([header[16], header[17]]),
        })
    }
}

#[derive(Debug)]
struct PageHeader {
    node_type: u8,
    first_freeblock: u16,
    number_of_cells: u16,
    content_start: u16,
}

impl PageHeader {
    fn read(file: &mut File) -> Result<Self> {
        let mut header = [0; 8];
        file.read_exact(&mut header)?;

        Ok(Self {
            node_type: u8::from_be_bytes([header[0]]),
            first_freeblock: u16::from_be_bytes([header[1], header[2]]),
            number_of_cells: u16::from_be_bytes([header[3], header[4]]),
            content_start: u16::from_be_bytes([header[5], header[6]]),
        })
    }
}

fn read_var_int(file: &mut File) -> Result<i64> {
    let mut value: i64 = 0;
    let mut byte = [0; 1];

    for i in 0..9 {
        file.read_exact(&mut byte)?;
        let byte = byte[0];

        if i == 8 {
            value = (value << 8) | byte as i64;
            break;
        } else {
            value = (value << 7) | byte as i64;
            if byte < 0b1000_0000 {
                break;
            }
        }
    }

    return Ok(value);
}

fn main() -> Result<()> {
    // Parse arguments
    let args = std::env::args().collect::<Vec<_>>();
    match args.len() {
        0 | 1 => bail!("Missing <database path> and <command>"),
        2 => bail!("Missing <command>"),
        _ => {}
    }

    // Parse command and act accordingly
    let command = &args[2];
    match command.as_str() {
        ".dbinfo" => {
            let mut file = File::open(&args[1])?;
            let header = Header::read(&mut file)?;

            println!("database page size: {}", header.page_size);

            let btree_header = PageHeader::read(&mut file)?;

            println!("number of tables: {}", btree_header.number_of_cells);
        }
        ".tables" => {
            let mut file = File::open(&args[1])?;

            let _header = Header::read(&mut file)?;
            let btree_header = PageHeader::read(&mut file)?;

            let mut cell_pointers = Vec::with_capacity(btree_header.number_of_cells.into());
            for _ in 0..btree_header.number_of_cells {
                let mut cell_pointer = [0; 2];
                file.read_exact(&mut cell_pointer)?;
                cell_pointers.push(u16::from_be_bytes([cell_pointer[0], cell_pointer[1]]));
            }

            for cell_pointer in cell_pointers.iter() {
                file.seek(SeekFrom::Start(*cell_pointer as u64))?;
                let _payload_bytes = read_var_int(&mut file)?;
                let _rowid = read_var_int(&mut file)?;

                let record = Record::read(&mut file, 5)?;

                if let ColumnValue::Text(ref name) = record.values[1] {
                    let name = String::from_utf8_lossy(&name);
                    if !name.starts_with("sqlite_") {
                        println!("{}", name);
                    }
                }
            }
        }
        _ => bail!("Missing or invalid command passed: {}", command),
    }

    Ok(())
}
