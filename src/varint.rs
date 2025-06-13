use crate::file_reader::BytesIterator;

pub fn decode(bytes_iterator: &mut BytesIterator) -> (u64, u64) {
    let mut integer: u64 = 0;
    let mut bytes_read: u64 = 0;
    loop {
        let val_64: u64 = bytes_iterator.next().unwrap().into();
        integer = integer << 7 | (val_64 & 0x7F);
        bytes_read += 1;
        if val_64 >> 7 == 0 {
            break;
        }
    }

    (integer, bytes_read)
}
