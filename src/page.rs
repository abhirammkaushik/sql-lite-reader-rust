const PAGE_SIZE: u16 = 4096;

use std::{u64, usize};

use crate::cell::{Cell, Record, RecordHeader, SerialType, SerialTypeInfo, TableIntCell, TableLeafCell};
use crate::{
    file_reader::{BytesIterator, FileReader},
    page_type::{get_page_type, PageType},
    varint,
};

#[derive(Debug)]
pub struct PageHeader {
    pub page_type: PageType,
    first_free_block: u16,
    pub cell_count: u16,
    cell_content_offset: u16,
    fragmented_bytes: u8,
    right_pointer: Option<u32>,
}

pub struct Page {
    pub page_header: PageHeader,
    pub cells: Box<[Box<dyn Cell>]>,
}

pub struct PageMetaData {
    pub page_type: PageType,
    pub page_header_size: usize,
}

impl Page {
    pub fn new(page_header: PageHeader, cells: Box<[Box<dyn Cell>]>) -> Self {
        Self { page_header, cells }
    }
}

pub struct PageReader {
    bytes_iterator: BytesIterator,
    page_start_offset: u64, // start of page on for file
    page_meta_data: PageMetaData,
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
        let page_meta_data = get_page_metadata(&mut bytes_iterator, page_number);
        PageReader {
            bytes_iterator,
            page_start_offset,
            page_meta_data,
        }
    }

    pub fn read_page(&mut self) -> Page {
        let page_header_size = self.page_meta_data.page_header_size;
        let page_type = self.page_meta_data.page_type;

        let page_header = self.get_page_header(page_header_size, &page_type);
        //println!("{:?}", page_header);

        if page_type == PageType::TblLeaf {
            let cells = self
                .read_table_leaf_cells(page_header.cell_count)
                .unwrap();
            Page { page_header, cells }
        } else if page_type == PageType::TblInt {
            let cells = self
                .read_table_int_cell(page_header.cell_count)
                .unwrap();
            Page { page_header, cells }
        } else {
            panic!("invalid page type {:?}", page_type);
        }
    }

    fn get_page_header(&mut self, page_header_size: usize, page_type: &PageType) -> PageHeader {
        // first bit was already read in get_page_metadata to determine page type
        let page_header = self.bytes_iterator.next_n(page_header_size - 1).unwrap();

        PageHeader {
            page_type: *page_type,
            first_free_block: u16::from_be_bytes([page_header[0], page_header[1]]),
            cell_count: u16::from_be_bytes([page_header[2], page_header[3]]),
            cell_content_offset: u16::from_be_bytes([page_header[4], page_header[5]]),
            fragmented_bytes: u8::from_be_bytes([page_header[6]]),
            right_pointer: None,
        }
    }

    fn read_table_int_cell(&mut self, cell_count: u16) -> Option<Box<[Box<dyn Cell>]>> {
        let mut cell_offsets_iterator = self
            .bytes_iterator
            .next_n_as_iter(cell_count as usize * 2_usize)
            .unwrap();

        let mut cells: Vec<Box<dyn Cell>> = Vec::new();
        while cell_offsets_iterator.has_next() {
            let cell_offset = cell_offsets_iterator.next_n(2).unwrap();
            let cell_offset = u16::from_be_bytes([cell_offset[0], cell_offset[1]]);
            let offset: u64 = cell_offset as u64;

            let bytes = self.bytes_iterator.jump_to(offset as usize).next_n(4).unwrap();
            let left_child_page_id = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
            let (row_id, _) = varint::decode(&mut self.bytes_iterator);
            cells.push(Box::new(TableIntCell { row_id: row_id as u16, left_child_page_id }));
        }

        Some(cells.into())
    }

    fn read_table_leaf_cells(&mut self, cell_count: u16) -> Option<Box<[Box<dyn Cell>]>> {
        let mut cell_offsets_iterator = self
            .bytes_iterator
            .next_n_as_iter(cell_count as usize * 2_usize)
            .unwrap();
        //println!("{:?}", cell_offsets_iterator);
        let mut cells: Vec<Box<dyn Cell>> = Vec::new();
        while cell_offsets_iterator.has_next() {
            let cell_offset = cell_offsets_iterator.next_n(2).unwrap();
            let cell_offset = u16::from_be_bytes([cell_offset[0], cell_offset[1]]);
            /* the cell grows up from the end of the cell content area
            while the contents of the cell grows down from the start of the cell content area */
            let mut offset: u64 = cell_offset as u64;
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

            cells.push(Box::new(TableLeafCell {
                record_size,
                row_id: row_id as u16,
                record,
            }));
        }
        Some(cells.into())
    }

    fn get_column_serial_type_info(&self, val: u64) -> SerialTypeInfo {
        if val == 0 {
            SerialTypeInfo {
                read_size: 0,
                serial_type: SerialType::NULL,
            }
        } else if val < 9 {
            SerialTypeInfo {
                read_size: val,
                serial_type: SerialType::INTEGER,
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

pub struct PageReaderBuilder {
    file_reader: FileReader,
    page_size: u16,
}

impl PageReaderBuilder {
    pub fn new(file_reader: FileReader, page_size: u16) -> Self {
        Self { file_reader, page_size }
    }
    pub fn new_reader(&mut self, page_number: u16) -> PageReader {
        PageReader::new(&mut self.file_reader, page_number, self.page_size)
    }
}

fn get_page_metadata(bytes_iterator: &mut BytesIterator, page_number: u16) -> PageMetaData {
    let bytes = bytes_iterator.next_n(1_usize).unwrap();
    let page_type = get_page_type(&bytes[0]);
    if page_type == PageType::IdxInt || page_type == PageType::TblInt {
        PageMetaData {
            page_type,
            page_header_size: 12,
        }
    } else {
        PageMetaData {
            page_type,
            page_header_size: 8,
        }
    }
}