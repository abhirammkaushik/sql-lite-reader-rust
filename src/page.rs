const PAGE_SIZE: u16 = 4096;

use std::{u64, usize};

use crate::{
    file_reader::{BytesIterator, FileReader},
    page_type::{get_page_type, PageType},
    varint,
};

#[derive(Debug)]
pub enum SerialType {
    NULL,
    BLOB,
    TEXT,
}

#[derive(Debug)]
pub struct SerialTypeInfo {
    read_size: u64,
    serial_type: SerialType,
}

#[derive(Debug)]
pub struct RecordHeader {
    header_size: u8,
    serial_types: Box<[SerialTypeInfo]>,
}

#[derive(Debug)]
pub struct Record {
    record_header: RecordHeader,
    pub rows: Box<[Box<[u8]>]>,
}

#[derive(Debug)]
pub struct Cell {
    pub record_size: u64,
    row_id: u16,
    pub record: Record,
}

#[derive(Debug)]
pub struct PageHeader {
    page_type: PageType,
    first_free_block: u16,
    pub cell_count: u16,
    cell_content_offset: u16,
    fragmented_bytes: u8,
}

pub struct Page {
    pub page_header: PageHeader,
    pub cells: Box<[Cell]>,
}

impl Page {
    pub fn new(page_header: PageHeader, cells: Box<[Cell]>) -> Self {
        Self { page_header, cells }
    }
}

pub struct PageReader {
    bytes_iterator: BytesIterator,
    page_start_offset: u64,
}

impl PageReader {
    pub fn new(file_reader: &mut FileReader, page_number: u16, page_size: u16) -> Self {
        let (page_start_offset, size) =
            ((page_size * (page_number - 1)) as u64, page_size as usize);

        let mut bytes_iterator = file_reader
            .read_bytes_from(page_start_offset, size)
            .unwrap();
        if page_number == 1 {
            bytes_iterator.jump_to(100_usize);
        }

        PageReader {
            bytes_iterator,
            page_start_offset,
        }
    }

    pub fn read_page(&mut self) -> Page {
        let page_header_size: usize = 8;
        let offset = self.page_start_offset;

        let page_header = self.get_page_header(page_header_size);
        //println!("{:?}", page_header);

        let cell_pointer_start = self
            .get_cell_pointer_start(&page_header.page_type, page_header_size.try_into().unwrap())
            .unwrap();

        //println!("cell pointer start: {cell_pointer_start}");
        let cells = self
            .read_cells(page_header.cell_count, cell_pointer_start as u64 + offset)
            .unwrap();
        Page { page_header, cells }
    }

    fn get_page_header(&mut self, page_header_size: usize) -> PageHeader {
        let page_header = self.bytes_iterator.next_n(page_header_size).unwrap();

        PageHeader {
            page_type: get_page_type(&page_header[0]),
            first_free_block: u16::from_be_bytes([page_header[1], page_header[2]]),
            cell_count: u16::from_be_bytes([page_header[3], page_header[4]]),
            cell_content_offset: u16::from_be_bytes([page_header[5], page_header[6]]),
            fragmented_bytes: u8::from_be_bytes([page_header[7]]),
        }
    }

    fn get_cell_pointer_start(&self, page_type: &PageType, page_header_size: u8) -> Option<u8> {
        match page_type {
            PageType::TblLeaf => Some(page_header_size),
            PageType::TblIdx => Some(page_header_size),
            PageType::IntLeaf => Some(page_header_size + 4),
            PageType::IntIdx => Some(page_header_size + 4),
            PageType::Invalid => None,
        }
    }

    fn read_cells(&mut self, cell_count: u16, cell_pointer_start: u64) -> Option<Box<[Cell]>> {
        // println!("cell pointer start with offset: {cell_pointer_start}");

        let mut cell_offsets_iterator = self
            .bytes_iterator
            .next_n_as_iter(cell_count as usize * 2_usize)
            .unwrap();
        //println!("{:?}", cell_offsets_iterator);
        let mut cells: Vec<Cell> = Vec::new();
        let mut cell_idx: u16 = 0;
        while cell_offsets_iterator.has_next() {
            let cell_offset = cell_offsets_iterator.next_n(2).unwrap();
            let cell_offset = u16::from_be_bytes([cell_offset[0], cell_offset[1]]);
            let mut offset: u64 = cell_offset as u64;
            cell_idx += 1;
            //println!("cell offset for cell {cell_idx} is {offset}");

            let (record_size, bytes_read) =
                varint::decode(self.bytes_iterator.jump_to(offset as usize));
            offset += bytes_read;

            //println!("record size: {record_size}, bytes read {bytes_read}, offset: {offset}");
            if record_size == 0 {
                return Some(Box::new([]));
            }

            let (row_id, bytes_read) = varint::decode(&mut self.bytes_iterator);
            offset += bytes_read;
            //println!("row id: {row_id}, bytes read {bytes_read}, offset: {offset}");

            let (mut record_header_size, bytes_read) = varint::decode(&mut self.bytes_iterator);

            offset += bytes_read;
            record_header_size -= bytes_read;

            //println!("record_header_size: {record_header_size}, bytes read {bytes_read}, offset: {offset}");

            let mut serial_types = Vec::new();
            let record_header_size_copy = record_header_size;
            let mut record_body_size: u64 = 0;
            //println!("record header_size {record_header_size}");
            while record_header_size > 0 {
                let (val, bytes_read) = varint::decode(&mut self.bytes_iterator);
                let serial_type_info: SerialTypeInfo = self.get_column_serial_type_info(val);
                record_body_size += serial_type_info.read_size;
                serial_types.push(serial_type_info);

                record_header_size -= bytes_read;
                offset += bytes_read;
            }

            let record_header = RecordHeader {
                header_size: record_header_size_copy as u8,
                serial_types: serial_types.into_boxed_slice(),
            };

            let mut rows: Vec<Box<[u8]>> = Vec::new();
            let mut record_body_iterator = self
                .bytes_iterator
                .next_n_as_iter(record_body_size as usize)
                .unwrap();
            for serial_type_info in record_header.serial_types.iter() {
                let row = record_body_iterator
                    .next_n(serial_type_info.read_size as usize)
                    .unwrap();

                rows.push(row);
            }

            let record = Record {
                record_header,
                rows: rows.into(),
            };

            //println!("{:?}", record);

            cells.push(Cell {
                record_size,
                row_id: row_id as u16,
                record,
            });
        }
        Some(cells.into())
    }

    fn get_column_serial_type_info(&self, val: u64) -> SerialTypeInfo {
        if val < 12 {
            SerialTypeInfo {
                read_size: 0,
                serial_type: SerialType::NULL,
            }
        } else if val % 2 == 0 {
            SerialTypeInfo {
                read_size: (val - 12) / 2,
                serial_type: SerialType::BLOB,
            }
        } else {
            SerialTypeInfo {
                read_size: (val - 13) / 2,
                serial_type: SerialType::TEXT,
            }
        }
    }
}
