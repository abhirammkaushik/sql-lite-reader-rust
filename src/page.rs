use crate::{
    file_reader::BytesIterator,
    page_type::{get_page_type, PageType}
    ,
};
use std::fmt::Display;
use std::ops::Deref;
use std::usize;

use core::any::Any;
use std::fmt::{Debug, Formatter};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SerialType {
    NULL,
    INTEGER0,
    INTEGER1,
    INTEGER(u64),
    FLOAT64(u64),
    BLOB(u64),
    TEXT(u64),
    RESERVED,
}

#[derive(Debug, Clone)]
pub struct SerialTypeInfo {
    pub read_size: u64,
    pub serial_type: SerialType,
}

#[derive(Debug, Clone)]
pub struct RecordHeader {
    pub header_size: u8,
    pub serial_types: Box<[SerialType]>,
}

#[derive(Debug, Clone)]
pub struct Record {
    pub record_header: RecordHeader,
    pub rows: Box<[Box<[u8]>]>,
}

pub trait CellClone {
    fn clone_cell(&self) -> Box<dyn Cell>; /* to help with cloning a cell */
}

pub trait Cell: CellClone + Display + Debug + 'static {
    fn as_any(&self) -> &dyn Any; /* to help with downcast */
}

impl<T: Cell + Clone> CellClone for T {
    fn clone_cell(&self) -> Box<dyn Cell> {
        Box::new(self.clone())
    }
}


#[derive(Debug, Clone)]
pub struct TableLeafCell {
    pub record_size: u64,
    pub row_id: u16,
    pub record: Record,
}

#[derive(Debug, Clone)]
pub struct TableIntCell {
    pub row_id: u16,
    pub left_child_page_no: u32,
}

impl Display for TableLeafCell {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "TableLeafCell {{ record_size: {}, row_id: {}, record: {:?} }}", self.record_size, self.row_id, self.record)
    }
}

impl Cell for TableLeafCell {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl Display for TableIntCell {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "TableIntCell {{ row_id: {}, left_child_page_no: {} }}", self.row_id, self.left_child_page_no)
    }
}

impl Cell for TableIntCell {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[inline]
pub fn downcast<T: Any + Cell>(x: &Box<dyn Cell>) -> &T {
    x.deref().as_any().downcast_ref::<T>().unwrap()
}
#[derive(Debug, Clone)]
pub struct PageHeader {
    pub page_type: PageType,
    pub first_free_block: u16,
    pub cell_count: u16,
    pub cell_content_offset: u16,
    pub fragmented_bytes: u8,
    pub right_pointer: Option<u32>,
}

#[derive(Debug)]
pub struct Page {
    pub page_header: PageHeader,
    pub cells: Box<[Box<dyn Cell>]>,
}

#[derive(Debug)]
pub struct PageMetaData {
    pub page_type: PageType,
    pub page_header_size: usize,
}

impl Page {
    pub fn new(page_header: PageHeader, cells: Box<[Box<dyn Cell>]>) -> Self {
        Self { page_header, cells }
    }
}

impl Clone for Page {
    fn clone(&self) -> Self {
        let cells: Vec<Box<dyn Cell>> = self.cells.iter().map(|cell| cell.clone_cell()).collect();
        let cells: Box<[Box<dyn Cell>]> = cells.into_boxed_slice();
        Self {
            page_header: self.page_header.clone(),
            cells,
        }
    }
}

impl Display for Page {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Page {{ page_header: {:?}, cells: {:?} }}", self.page_header, self.cells)
    }
}

pub fn get_page_metadata(bytes_iterator: &mut BytesIterator) -> PageMetaData {
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

pub fn get_read_size(serial_type: &SerialType) -> u64 {
    match serial_type {
        SerialType::INTEGER0 => 0,
        SerialType::INTEGER1 => 0,
        SerialType::NULL => 0,
        SerialType::INTEGER(size) => *size,
        SerialType::FLOAT64(size) => *size,
        SerialType::BLOB(size) => *size,
        SerialType::TEXT(size) => *size,
        _ => panic!(
            "invalid serial type info {:?}",
            serial_type
        )
    }
}