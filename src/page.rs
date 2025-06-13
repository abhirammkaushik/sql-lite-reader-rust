use crate::{
    file_reader::BytesIterator,
    page_type::{get_page_type, PageType},
};
use std::fmt::Display;

use core::any::Any;
use std::fmt::{Debug, Formatter};
use std::ops::Deref;

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
pub struct RecordHeader {
    pub header_size: u8,
    pub serial_types: Box<[SerialType]>,
}

#[derive(Debug, Clone)]
pub struct Record {
    pub record_header: RecordHeader,
    pub rows: Vec<String>,
}

pub trait CellClone {
    fn clone_cell(&self) -> Box<dyn Cell>; /* to help with cloning a cell */
}

pub trait Cell: CellClone + Display + Debug + 'static {
    fn as_any(&self) -> &dyn Any; /* to help with downcast */

    fn record(&self) -> Option<Record> {
        None
    }

    fn left_child_page_no(&self) -> Option<u32> {
        None
    }

    fn row_id(&self) -> Option<i64> {
        None
    }
}

impl<T: Cell + Clone> CellClone for T {
    fn clone_cell(&self) -> Box<dyn Cell> {
        Box::new(self.clone())
    }
}

#[derive(Debug, Clone)]
pub struct TableLeafCell {
    pub record_size: u64,
    pub row_id: i64,
    pub record: Record,
}

#[derive(Debug, Clone)]
pub struct TableIntCell {
    pub row_id: i64,
    pub left_child_page_no: u32,
}

#[derive(Debug, Clone)]
pub struct IdxLeafCell {
    pub record_size: u64,
    pub record: Record,
}

#[derive(Debug, Clone)]
pub struct IdxIntCell {
    pub left_child_page_no: u32,
    pub record_size: u64,
    pub record: Record,
}

impl Display for TableLeafCell {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "TableLeafCell {{ record_size: {}, row_id: {}, record: {:?} }}",
            self.record_size, self.row_id, self.record
        )
    }
}

impl Cell for TableLeafCell {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn record(&self) -> Option<Record> {
        Some(self.record.clone())
    }

    fn row_id(&self) -> Option<i64> {
        Some(self.row_id)
    }
}

impl Display for TableIntCell {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "TableIntCell {{ row_id: {}, left_child_page_no: {} }}",
            self.row_id, self.left_child_page_no
        )
    }
}

impl Cell for TableIntCell {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn left_child_page_no(&self) -> Option<u32> {
        Some(self.left_child_page_no)
    }

    fn row_id(&self) -> Option<i64> {
        Some(self.row_id)
    }
}

impl Display for IdxLeafCell {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "IdxLeafCell {{ record_size: {}, record: {:?} }}",
            self.record_size, self.record
        )
    }
}

impl Cell for IdxLeafCell {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn record(&self) -> Option<Record> {
        Some(self.record.clone())
    }
}

impl Display for IdxIntCell {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "IdxIntCell {{ left_child_page_no: {}, record_size: {}, record: {:?} }}",
            self.left_child_page_no, self.record_size, self.record
        )
    }
}

impl Cell for IdxIntCell {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn record(&self) -> Option<Record> {
        Some(self.record.clone())
    }

    fn left_child_page_no(&self) -> Option<u32> {
        Some(self.left_child_page_no)
    }
}

#[inline]
pub fn downcast<T: Any + Cell>(x: &Box<dyn Cell>) -> Option<&T> {
    x.deref().as_any().downcast_ref::<T>()
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
        write!(
            f,
            "Page {{ page_header: {:?}, cells: {:?} }}",
            self.page_header, self.cells
        )
    }
}

pub fn get_page_metadata(bytes_iterator: &mut BytesIterator) -> PageMetaData {
    let byte = bytes_iterator.next().unwrap();
    let page_type = get_page_type(&byte);
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
        _ => panic!("invalid serial type info {:?}", serial_type),
    }
}

pub enum SearchResult {
    ThisPage(Box<dyn Cell>, u32),
    LeftPage(Box<dyn Cell>),
    RightPage,
}
