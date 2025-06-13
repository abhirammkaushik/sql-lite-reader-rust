use crate::file_reader::{BytesIterator, FileReader};
use crate::page::SerialType::RESERVED;
use crate::page::{
    get_read_size, Cell, IdxIntCell, IdxLeafCell, Page, PageHeader, PageMetaData, Record,
    RecordHeader, SerialType, TableIntCell, TableLeafCell,
};
use crate::page_type::PageType;
use crate::{page, varint};
pub struct PageReader {
    bytes_iterator: BytesIterator,
    pub page_meta_data: PageMetaData,
}

impl PageReader {
    pub fn new(file_reader: &mut FileReader, page_number: u32, page_size: u16) -> Self {
        let (page_start_offset, size) = (
            page_size as u64 * (page_number as u64 - 1),
            page_size as usize,
        );

        let mut bytes_iterator = file_reader
            .read_bytes_from(page_start_offset, size)
            .unwrap();
        // let bytes = bytes_iterator.next_n(page_size as usize).unwrap();
        // println!("{:?}", bytes);
        // bytes_iterator.jump_to(0);
        if page_number == 1 {
            bytes_iterator.jump_to(100_usize);
        }
        let page_meta_data = page::get_page_metadata(&mut bytes_iterator);
        // println!("reading page {page_number} at offset {page_start_offset} with size {size} {:?}", page_meta_data);
        PageReader {
            bytes_iterator,
            page_meta_data,
        }
    }

    pub fn read_page(&mut self) -> Page {
        let page_header_size = self.page_meta_data.page_header_size;
        let page_type = self.page_meta_data.page_type;

        let page_header = self.get_page_header(page_header_size, &page_type);

        let cells: Box<[Box<dyn Cell>]> = match page_type {
            PageType::TblLeaf => self.read_table_leaf_cells(page_header.cell_count).unwrap(),
            PageType::TblInt => self.read_table_int_cell(page_header.cell_count).unwrap(),
            PageType::IdxLeaf => self.read_index_leaf_cells(page_header.cell_count).unwrap(),
            PageType::IdxInt => self.read_index_int_cell(page_header.cell_count).unwrap(),
            PageType::Invalid => panic!("Invalid page type"),
        };

        Page { page_header, cells }
    }

    fn get_page_header(&mut self, page_header_size: usize, page_type: &PageType) -> PageHeader {
        // the first bit was already read in get_page_metadata to determine a page type
        let page_header = self.bytes_iterator.next_n(page_header_size - 1).unwrap();
        let right_pointer = if page_type == &PageType::TblInt || page_type == &PageType::IdxInt {
            Option::from(u32::from_be_bytes(page_header[7..11].try_into().unwrap()))
        } else {
            None
        };

        PageHeader {
            page_type: *page_type,
            first_free_block: u16::from_be_bytes([page_header[0], page_header[1]]),
            cell_count: u16::from_be_bytes([page_header[2], page_header[3]]),
            cell_content_offset: u16::from_be_bytes([page_header[4], page_header[5]]),
            fragmented_bytes: u8::from_be_bytes([page_header[6]]),
            right_pointer,
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
            let cell_offset: usize = u16::from_be_bytes([cell_offset[0], cell_offset[1]]).into();

            let bytes = self.bytes_iterator.jump_to(cell_offset).next_n(4).unwrap();
            let left_child_page_id = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
            let (row_id, _) = varint::decode(&mut self.bytes_iterator);
            cells.push(Box::new(TableIntCell {
                row_id: row_id as i64,
                left_child_page_no: left_child_page_id,
            }));
        }

        Some(cells.into())
    }

    fn read_table_leaf_cells(&mut self, cell_count: u16) -> Option<Box<[Box<dyn Cell>]>> {
        let mut cell_offsets_iterator = self
            .bytes_iterator
            .next_n_as_iter(cell_count as usize * 2_usize)
            .unwrap();
        // println!("{:?}", cell_offsets_iterator);
        let mut cells: Vec<Box<dyn Cell>> = Vec::new();
        while cell_offsets_iterator.has_next() {
            let cell_offset = cell_offsets_iterator.next_n(2).unwrap();
            let cell_offset: usize = u16::from_be_bytes([cell_offset[0], cell_offset[1]]).into();
            /* the cell grows up from the end of the cell content area
            while the contents of the cell grows down from the start of the cell content area */

            let (record_size, _) = varint::decode(self.bytes_iterator.jump_to(cell_offset));

            let (row_id, _) = varint::decode(&mut self.bytes_iterator);
            let record = self.read_record(true).unwrap();

            //println!("{:?}", record);

            cells.push(Box::new(TableLeafCell {
                record_size,
                row_id: row_id as i64,
                record,
            }));
        }
        Some(cells.into())
    }

    fn read_index_int_cell(&mut self, cell_count: u16) -> Option<Box<[Box<dyn Cell>]>> {
        let mut cell_offsets_iterator = self
            .bytes_iterator
            .next_n_as_iter(cell_count as usize * 2_usize)
            .unwrap();
        let mut cells: Vec<Box<dyn Cell>> = Vec::new();
        while cell_offsets_iterator.has_next() {
            let cell_offset = cell_offsets_iterator.next_n(2).unwrap();
            let cell_offset: usize = u16::from_be_bytes([cell_offset[0], cell_offset[1]]).into();

            let bytes = self.bytes_iterator.jump_to(cell_offset).next_n(4).unwrap();
            let left_child_page_no = u32::from_be_bytes(bytes[0..=3].try_into().unwrap());
            let (record_size, _) = varint::decode(&mut self.bytes_iterator);
            let record = self.read_record(false);
            if record.is_none() {
                continue;
            }
            let record = record.unwrap();

            cells.push(Box::new(IdxIntCell {
                record_size,
                left_child_page_no,
                record,
            }))
        }
        Some(cells.into())
    }

    fn read_index_leaf_cells(&mut self, cell_count: u16) -> Option<Box<[Box<dyn Cell>]>> {
        let mut cell_offsets_iterator = self
            .bytes_iterator
            .next_n_as_iter(cell_count as usize * 2_usize)
            .unwrap();
        let mut cells: Vec<Box<dyn Cell>> = Vec::new();
        while cell_offsets_iterator.has_next() {
            let cell_offset = cell_offsets_iterator.next_n(2).unwrap();
            let cell_offset: usize = u16::from_be_bytes([cell_offset[0], cell_offset[1]]).into();
            let (record_size, _) = varint::decode(self.bytes_iterator.jump_to(cell_offset));

            let record = self.read_record(true).unwrap();
            //println!("{:?}", record);

            cells.push(Box::new(IdxLeafCell {
                record_size,
                record,
            }));
        }
        Some(cells.into())
    }

    fn read_record(&mut self, null_allowed: bool) -> Option<Record> {
        let (mut record_header_size, bytes_read) = varint::decode(&mut self.bytes_iterator);

        record_header_size -= bytes_read;

        let mut serial_types = Vec::new();
        let record_header_size_copy = record_header_size;
        let mut record_body_size: u64 = 0;
        //println!("record header_size {record_header_size}");
        while record_header_size > 0 {
            let (val, bytes_read) = varint::decode(&mut self.bytes_iterator);
            let serial_type: SerialType = self.get_column_serial_type_info(val);
            let size = get_read_size(&serial_type);
            // println!("{}, {:?}, {}", size, serial_type, bytes_read);
            record_body_size += size;
            serial_types.push(serial_type);

            record_header_size -= bytes_read;
        }

        let record_header = RecordHeader {
            header_size: record_header_size_copy as u8,
            serial_types: serial_types.into_boxed_slice(),
        };

        let mut rows: Vec<String> = Vec::new();
        let mut record_body_iterator = self
            .bytes_iterator
            .next_n_as_iter(record_body_size as usize)
            .unwrap();

        for serial_type in record_header.serial_types.iter() {
            let read_size = get_read_size(serial_type);
            if !null_allowed && serial_type == &SerialType::NULL {
                return None;
            }

            if read_size == 0 {
                rows.push(String::new());
                continue;
            }
            let row = decode(
                serial_type,
                &record_body_iterator.next_n(read_size as usize).unwrap(),
            );

            rows.push(row);
        }

        Some(Record {
            record_header,
            rows,
        })
    }

    fn get_column_serial_type_info(&self, val: u64) -> SerialType {
        if val == 0 {
            SerialType::NULL
        } else if val < 12 {
            if val == 1 {
                SerialType::INTEGER(1)
            } else if val == 2 {
                SerialType::INTEGER(2)
            } else if val == 3 {
                SerialType::INTEGER(3)
            } else if val == 4 {
                SerialType::INTEGER(4)
            } else if val == 5 {
                SerialType::INTEGER(6)
            } else if val == 6 {
                SerialType::INTEGER(8)
            } else if val == 7 {
                SerialType::FLOAT64(8)
            } else if val == 8 {
                SerialType::INTEGER0
            } else if val == 9 {
                SerialType::INTEGER1
            } else {
                RESERVED
            }
        } else if val % 2 == 0 {
            SerialType::BLOB((val - 12) / 2)
        } else {
            SerialType::TEXT((val - 13) / 2)
        }
    }
}

pub struct PageReaderBuilder {
    file_reader: FileReader,
    page_size: u16,
}

impl PageReaderBuilder {
    pub fn new(file_reader: FileReader, page_size: u16) -> Self {
        Self {
            file_reader,
            page_size,
        }
    }
    pub fn new_reader(&mut self, page_number: u32) -> PageReader {
        PageReader::new(&mut self.file_reader, page_number, self.page_size)
    }
}

fn decode(serial_type: &SerialType, row: &[u8]) -> String {
    match serial_type {
        SerialType::INTEGER(size) => row_u64_converter(row, *size).to_string(),
        SerialType::TEXT(_size) | SerialType::BLOB(_size) => {
            String::from_utf8_lossy(row).to_string()
        }
        SerialType::FLOAT64(_size) => f64::from_be_bytes([
            row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7],
        ])
        .to_string(),
        _ => String::new(),
    }
}
fn row_u64_converter(row: &[u8], n: u64) -> u64 {
    match n {
        1 => u8::from_be_bytes([row[0]]) as u64,
        2 => u16::from_be_bytes([row[0], row[1]]) as u64,
        3 => u32::from_be_bytes([0_u8, row[0], row[1], row[2]]) as u64,
        4 => u32::from_be_bytes([row[0], row[1], row[2], row[3]]) as u64,
        6 => u64::from_be_bytes([0_u8, 0_u8, row[0], row[1], row[2], row[3], row[4], row[5]]),
        8 => u64::from_be_bytes([
            row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7],
        ]),
        _ => 0_u64,
    }
}
