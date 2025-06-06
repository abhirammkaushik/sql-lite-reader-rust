use std::fs::File;
use std::io::{self, BufReader, Read, Seek};
use std::os::unix::fs::FileExt;

pub struct FileReader {
    reader: BufReader<File>,
    last_offset: u64,
}

// initailizer
impl FileReader {
    pub fn new(path: &String) -> Option<Self> {
        let f = File::open(path).expect("cannot open file in path {path}");
        Some(FileReader {
            reader: BufReader::with_capacity(u16::MAX as usize, f),
            last_offset: 0_u64,
        })
    }

    pub fn read_bytes_from(&mut self, offset: u64, size: usize) -> io::Result<BytesIterator> {
        //println!("{offset} {:?}", self.last_offset);
        if offset > self.last_offset {
            self.read_from_offset_raw(offset, size)
        } else {
            self.read_from_offset(offset, size)
        }
    }

    fn read_from_offset_raw(&mut self, offset: u64, size: usize) -> io::Result<BytesIterator> {
        let f_reader = self.reader.get_mut();
        let mut bytes = vec![0_u8; size].into_boxed_slice();
        match f_reader.read_exact_at(&mut bytes, offset) {
            Ok(_) => {
                self.last_offset = offset + size as u64;
                io::Result::Ok(BytesIterator::new(bytes))
            }
            Err(err) => io::Result::Err(err),
        }
    }

    fn read_from_offset(&mut self, offset: u64, size: usize) -> io::Result<BytesIterator> {
        let _ = self.reader.seek(std::io::SeekFrom::Start(offset));
        self.read_bytes(size)
    }

    pub fn read_bytes(&mut self, size: usize) -> io::Result<BytesIterator> {
        let mut bytes = vec![0_u8; size].into_boxed_slice();
        match self.reader.read_exact(&mut bytes) {
            Ok(_) => {
                self.last_offset += size as u64;
                io::Result::Ok(BytesIterator::new(bytes))
            }
            Err(err) => {
                println!("{:?}", bytes);
                io::Result::Err(err)
            }
        }
    }
}

#[derive(Debug)]
pub struct BytesIterator {
    bytes: Box<[u8]>,
    offset: usize,
}

impl BytesIterator {
    #[inline]
    pub fn new(bytes: Box<[u8]>) -> Self {
        Self { bytes, offset: 0 }
    }

    #[inline]
    fn within_bounds(&self, n: &usize) -> bool {
        *n != 0usize || self.offset < self.bytes.len()
    }

    #[inline]
    pub fn next_n(&mut self, n: usize) -> Option<Box<[u8]>> {
        self.offset += n;
        self.peek_back_n(&n)
    }

    pub fn next_n_as_iter(&mut self, n: usize) -> Option<Self> {
        let bytes = self.next_n(n).unwrap();
        Some(Self::new(bytes))
    }

    #[inline]
    pub fn peek_back_n(&self, n: &usize) -> Option<Box<[u8]>> {
        if !self.within_bounds(n) {
            return None;
        }

        let start = self.offset - n;
        let end = self.offset;
        Some(self.bytes[start..end].to_vec().into_boxed_slice())
    }

    #[inline]
    pub fn from_offset(&mut self, start: usize, n: usize) -> Option<Box<[u8]>> {
        self.offset = start + n;
        self.peek_back_n(&n)
    }

    pub fn has_next(&self) -> bool {
        self.offset < self.bytes.len()
    }

    pub fn jump_to(&mut self, n: usize) -> &mut Self {
        self.offset = n;
        self
    }
}

impl Iterator for BytesIterator {
    type Item = u8;

    fn next(&mut self) -> Option<Self::Item> {
        self.offset += 1;
        Some(self.bytes[self.offset - 1])
    }
}
