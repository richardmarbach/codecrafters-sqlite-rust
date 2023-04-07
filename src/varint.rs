pub fn read(bytes: &[u8]) -> (i64, usize) {
    let mut varint = 0;
    let mut bytes_read = 0;

    for (i, byte) in bytes.iter().enumerate().take(9) {
        bytes_read += 1;

        if i == 8 {
            varint = (varint << 8) | *byte as i64;
            break;
        } else {
            varint = (varint << 7) | (*byte & 0b0111_1111) as i64;
            if *byte < 0b1000_0000 {
                break;
            }
        }
    }

    (varint, bytes_read)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_one_byte_varint() {
        assert_eq!(read(&[0b0000_0001]), (1, 1));
        assert_eq!(read(&[0b0000_0011]), (3, 1));
        assert_eq!(read(&[0b0111_1111]), (127, 1));
    }

    #[test]
    fn read_two_byte_varint() {
        assert_eq!(read(&[0b1000_0001, 0b0000_0000]), (128, 2));
        assert_eq!(read(&[0b1000_0001, 0b0000_0001]), (129, 2));
        assert_eq!(read(&[0b1000_0001, 0b0111_1111]), (255, 2));
    }

    #[test]
    fn read_nine_byte_varint() {
        assert_eq!(read(&vec![0xff; 9]), (-1, 9));
    }

    #[test]
    fn read_varint_from_longer_bytes() {
        assert_eq!(read(&vec![0x01; 10]), (1, 1));
        assert_eq!(read(&vec![0xff; 10]), (-1, 9));
    }
}
