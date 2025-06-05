#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum PageType {
    IdxInt,
    IdxLeaf,
    TblInt,
    TblLeaf,
    Invalid,
}

pub fn get_page_type(page_type: &u8) -> PageType {
    match page_type {
        2 => PageType::IdxInt,
        5 => PageType::IdxLeaf,
        10 => PageType::TblInt,
        13 => PageType::TblLeaf,
        _ => PageType::Invalid,
    }
}
