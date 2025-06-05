use core::any::Any;
#[derive(Debug)]
pub enum SerialType {
    NULL,
    INTEGER,
    BLOB,
    TEXT,
}

#[derive(Debug)]
pub struct SerialTypeInfo {
    pub read_size: u64,
    pub serial_type: SerialType,
}

#[derive(Debug)]
pub struct RecordHeader {
    pub header_size: u8,
    pub serial_types: Box<[SerialTypeInfo]>,
}

#[derive(Debug)]
pub struct Record {
    pub record_header: RecordHeader,
    pub rows: Box<[Box<[u8]>]>,
}

pub trait Cell {
    fn as_any(&self) -> &dyn Any;
}

#[derive(Debug)]
pub struct TableLeafCell {
    pub record_size: u64,
    pub row_id: u16,
    pub record: Record,
}

#[derive(Debug)]
pub struct TableIntCell {
    pub row_id: u16,
    pub left_child_page_id: u32,
}


impl Cell for TableLeafCell {
    fn as_any(&self) -> &dyn Any {
        self
    }
}
impl Cell for TableIntCell {
    fn as_any(&self) -> &dyn Any {
        self
    }
}
