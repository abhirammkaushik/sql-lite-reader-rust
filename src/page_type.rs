#[derive(Debug)]
pub enum PageType {
    IntIdx,
    IntLeaf,
    TblIdx,
    TblLeaf,
    Invalid,
}

pub fn get_page_type(page_type: &u8) -> PageType {
    match page_type {
        2 => PageType::IntIdx,
        5 => PageType::IntLeaf,
        10 => PageType::TblIdx,
        13 => PageType::TblLeaf,
        _ => PageType::Invalid,
    }
}
