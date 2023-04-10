use crate::varint;

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
pub enum ColumnValue<'page> {
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
    Blob(&'page [u8]),
    Text(&'page [u8]),
}

impl<'page> std::fmt::Display for ColumnValue<'page> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ColumnValue::Null => write!(f, "NULL"),
            ColumnValue::I8(n)
            | ColumnValue::I16(n)
            | ColumnValue::I24(n)
            | ColumnValue::I32(n)
            | ColumnValue::I48(n)
            | ColumnValue::I64(n) => write!(f, "{}", n),
            ColumnValue::F64(n) => write!(f, "{}", n),
            ColumnValue::Zero => write!(f, "0"),
            ColumnValue::One => write!(f, "1"),
            ColumnValue::Blob(content) => write!(f, "<BLOB {} bytes>", content.len()),
            ColumnValue::Text(content) => write!(f, "{}", String::from_utf8_lossy(content)),
        }
    }
}

#[derive(Debug)]
pub struct Record<'page> {
    pub values: Vec<ColumnValue<'page>>,
}

macro_rules! read_n_bytes_as_i64 {
    ($payload:expr, $cursor:expr, $n:expr) => {{
        let mut bytes = [0; 8];
        bytes[(8 - $n)..].copy_from_slice(&$payload[$cursor..$cursor + $n]);
        $cursor += $n;
        i64::from_be_bytes(bytes)
    }};
}

impl<'page> Record<'page> {
    pub fn read(payload: &'page [u8], column_count: usize) -> Self {
        let mut columns = Vec::with_capacity(column_count);

        let mut cursor = 0;
        let (_header_size, offset) = varint::read(&payload[cursor..]);
        cursor += offset;

        for _ in 0..column_count {
            let (column, offset) = varint::read(&payload[cursor..]);
            cursor += offset;
            columns.push(ColumnType::from(column as u64));
        }

        let mut values = Vec::with_capacity(column_count);
        for column in columns.iter() {
            let value = match column {
                ColumnType::Null => ColumnValue::Null,
                ColumnType::I8 => {
                    let value = ColumnValue::I8(read_n_bytes_as_i64!(payload, cursor, 1));
                    value
                }
                ColumnType::I16 => ColumnValue::I16(read_n_bytes_as_i64!(payload, cursor, 2)),
                ColumnType::I24 => ColumnValue::I24(read_n_bytes_as_i64!(payload, cursor, 3)),
                ColumnType::I32 => ColumnValue::I32(read_n_bytes_as_i64!(payload, cursor, 4)),
                ColumnType::I48 => ColumnValue::I48(read_n_bytes_as_i64!(payload, cursor, 6)),
                ColumnType::I64 => ColumnValue::I64(read_n_bytes_as_i64!(payload, cursor, 8)),
                ColumnType::F64 => {
                    let mut bytes = [0; 8];
                    bytes.copy_from_slice(&payload[cursor..cursor + 8]);
                    let value = ColumnValue::F64(f64::from_be_bytes(bytes));
                    cursor += 8;
                    value
                }
                ColumnType::Zero => ColumnValue::Zero,
                ColumnType::One => ColumnValue::One,
                ColumnType::Blob(size) => {
                    let value = ColumnValue::Blob(&payload[cursor..(cursor + *size)]);
                    cursor += *size;
                    value
                }
                ColumnType::Text(size) => {
                    eprintln!(
                        "cursor: {} slice:{}, size: {}",
                        cursor,
                        payload.len(),
                        *size
                    );
                    let value = ColumnValue::Text(&payload[cursor..(cursor + *size)]);
                    cursor += *size;
                    value
                }
            };
            values.push(value);
        }

        Record { values }
    }
}
