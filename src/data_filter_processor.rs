use crate::page::{
    downcast, Cell, IdxIntCell, IdxLeafCell, Page, SearchResult, TableIntCell, TableLeafCell,
};
use crate::page_reader::PageReaderBuilder;
use crate::page_type::PageType;
use crate::parser::QueryDetails;
use anyhow::bail;
use std::any::Any;
use std::cmp::Ordering;
use std::collections::{HashSet, VecDeque};
use std::ops::Deref;
use std::rc::Rc;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Filter {
    pub filter_col_pos: isize,
    pub filter_value: FilterValue,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum FilterValue {
    String(String),
    Int(i64),
}

pub fn perform_index_scan(
    root_index_page_cell: &dyn Cell,
    root_leaf_page_cell: &dyn Cell,
    builder: &mut PageReaderBuilder,
    select_col_names: Vec<String>,
    table_query_details: QueryDetails,
    filter: &Filter,
) {
    let (_page_no, root_index_page) = fetch_table_first_page(root_index_page_cell, builder);
    // println!("first index page no {:?}", _page_no);
    let (_page_no, root_table_page) = fetch_table_first_page(root_leaf_page_cell, builder);
    // println!("first table page no {:?}", _page_no);

    let record_ids = fetch_indexed_rows(root_index_page, builder, &filter.filter_value);
    // println!("{:?}", record_ids);
    fetch_rows_with_id(
        root_table_page,
        record_ids,
        builder,
        select_col_names,
        table_query_details,
        filter,
    );
}

pub fn perform_full_table_scan(
    root_page_cell: &dyn Cell,
    builder: &mut PageReaderBuilder,
    select_col_names: Vec<String>,
    table_query_details: QueryDetails,
    filter: Filter,
) {
    let (page_no, page) = fetch_table_first_page(root_page_cell, builder);
    let page_num_and_page: Vec<(u32, Page)> = fetch_all_leaves_for_table(page, builder, page_no);
    let col_positions = get_column_position(select_col_names, &table_query_details.stmt.columns);
    for page in &page_num_and_page {
        let unique_rows_sub = fetch_table_data(&col_positions, page, &filter).unwrap();
        unique_rows_sub.iter().for_each(|row| {
            println!("{}", *row);
        });
    }
}

pub fn count_all_rows(root_leaf_page_cell: &dyn Cell, builder: &mut PageReaderBuilder) {
    let (page_no, page) = fetch_table_first_page(root_leaf_page_cell, builder);
    let page_num_and_page: Vec<(u32, Page)> = fetch_all_leaves_for_table(page, builder, page_no);
    println!(
        "{:?}",
        page_num_and_page
            .iter()
            .map(|(_, page)| page.page_header.cell_count as u64)
            .sum::<u64>()
    );
}

fn fetch_indexed_rows(
    root_index_page: Page,
    builder: &mut PageReaderBuilder,
    filter_value: &FilterValue,
) -> Vec<String> {
    let mut page = root_index_page;
    let mut page_to_read = 0u32;
    let mut record_ids = Vec::new();
    let payload_extractor_fn =
        |cell: &dyn Cell| -> String { cell.record().unwrap().rows.first().unwrap().to_string() };

    while page.page_header.page_type == PageType::IdxInt {
        let cells = page.cells.deref();
        let res = bin_search_payload::<IdxIntCell>(cells, filter_value, &payload_extractor_fn);
        page_to_read = match res {
            SearchResult::ThisPage(cell, _) => {
                let (_, row_id) = get_payload_id(&*cell);
                record_ids.push(row_id);
                cell.left_child_page_no().unwrap()
            }
            SearchResult::LeftPage(cell) => cell.left_child_page_no().unwrap(),
            SearchResult::RightPage => page.page_header.right_pointer.unwrap(),
        };
        page = builder.new_reader(page_to_read).read_page();
    }

    let cell_idx = match bin_search_payload::<IdxLeafCell>(
        page.cells.deref(),
        filter_value,
        &payload_extractor_fn,
    ) {
        SearchResult::ThisPage(_, cell_idx) => cell_idx as i64,
        SearchResult::LeftPage(_) | SearchResult::RightPage => -1,
    };

    if cell_idx < 0 {
        return vec![];
    }

    let mut cell_idx = cell_idx as usize;
    'outer: loop {
        let cells = page.cells.deref();
        let len = page.cells.deref().len();
        for cell in cells.iter().take(len).skip(cell_idx) {
            let (payload, row_id) = get_payload_id(cell.deref());
            if filter_cmp(filter_value, &payload) == Ordering::Less {
                break 'outer;
            }
            record_ids.push(row_id);
        }
        page_to_read += 1;
        page = builder.new_reader(page_to_read).read_page();
        cell_idx = 0;
    }

    record_ids
}

fn fetch_rows_with_id(
    root_table_page: Page,
    record_ids: Vec<String>,
    builder: &mut PageReaderBuilder,
    select_col_names: Vec<String>,
    table_query_details: QueryDetails,
    filter: &Filter,
) {
    let col_positions = get_column_position(select_col_names, &table_query_details.stmt.columns);
    let root_page_rc = Rc::new(root_table_page.clone()); // use rc to avoid cloning the page repeatedly
    let payload_extractor_fn = |cell: &dyn Cell| -> String {
        cell.row_id()
            .expect("Failed to convert row_id to string")
            .to_string()
    };
    for row_id in record_ids {
        let mut page = root_page_rc.clone();
        let mut page_to_read = 0u32;
        let filter_row_id = FilterValue::Int(row_id.parse().unwrap());
        while page.page_header.page_type == PageType::TblInt {
            let cells = page.cells.deref();
            let res =
                bin_search_payload::<TableIntCell>(cells, &filter_row_id, &payload_extractor_fn);
            page_to_read = match res {
                SearchResult::ThisPage(cell, _) | SearchResult::LeftPage(cell) => {
                    cell.left_child_page_no().unwrap()
                }
                SearchResult::RightPage => page.page_header.right_pointer.unwrap(),
            };
            page = Rc::from(builder.new_reader(page_to_read).read_page());
        }

        let cell = match bin_search_payload::<TableLeafCell>(
            page.cells.deref(),
            &filter_row_id,
            &payload_extractor_fn,
        ) {
            SearchResult::ThisPage(cell, _) => cell,
            SearchResult::LeftPage(_) | SearchResult::RightPage => {
                panic!("interior page contains entry but leaf doesn't. Page no: {page_to_read}")
            }
        };

        if let Some(row) = filter_rows(
            filter,
            downcast::<TableLeafCell>(&cell).unwrap(),
            &col_positions,
        ) {
            println!("{}", row);
        }
    }
}

fn fetch_all_leaves(
    first_page: Page,
    builder: &mut PageReaderBuilder,
    first_page_no: u32,
) -> Vec<(u32, Page)> {
    let mut pages = vec![];
    let mut stack = VecDeque::new();
    let mut visited = HashSet::new();
    stack.push_back((first_page_no, first_page));

    let mut check_and_push = |page_no: u32, stack: &mut VecDeque<(u32, Page)>| {
        let mut reader = builder.new_reader(page_no);
        let page = reader.read_page();
        if !visited.contains(&page_no) {
            visited.insert(page_no);
            if reader.page_meta_data.page_type == PageType::TblLeaf {
                pages.push((page_no, page));
            } else {
                stack.push_back((page_no, page));
            }
        }
    };

    while !stack.is_empty() {
        let (_, int_page) = stack.pop_front().unwrap();
        if let Some(right_page_no) = int_page.page_header.right_pointer {
            check_and_push(right_page_no, &mut stack);
        }
        int_page.cells.iter().for_each(|cell| {
            let cell = downcast::<TableIntCell>(cell).unwrap();
            let left_page_no = cell.left_child_page_no;
            check_and_push(left_page_no, &mut stack);
        });
    }
    pages
}

fn filter_rows(filter: &Filter, cell: &TableLeafCell, col_positions: &[usize]) -> Option<String> {
    let rows = &cell.record.rows;
    let mut row_str = Vec::new();

    if filter.filter_col_pos == -1 || decode_match(filter, rows) {
        col_positions.iter().for_each(|&pos| {
            /* fetch based on the column position mentioned in the select query */
            let val = if pos == 0 {
                cell.row_id.to_string()
            } else {
                let row = &rows[pos];
                row.clone()
            };
            row_str.push(val);
        });
        return Some(row_str.join("|"));
    }
    None
}

pub fn fetch_all_leaves_for_table(
    first_page: Page,
    builder: &mut PageReaderBuilder,
    page_no: u32,
) -> Vec<(u32, Page)> {
    let page_type = first_page.page_header.page_type;
    if page_type == PageType::TblLeaf || page_type == PageType::IdxLeaf {
        vec![(page_no, first_page.clone())]
    } else if first_page.page_header.page_type == PageType::TblInt {
        // look for table leaves
        fetch_all_leaves(first_page, builder, page_no)
    } else {
        panic!("Invalid page type {:?}", page_type);
    }
}

fn bin_search_payload<T: Any + Cell>(
    cells: &[Box<dyn Cell>],
    filter_value: &FilterValue,
    payload_extractor_fn: &dyn Fn(&dyn Cell) -> String,
) -> SearchResult {
    let len = cells.len() as u32;
    let (mut l, mut h) = (0u32, len);
    let mut ret = SearchResult::RightPage;
    while l < h {
        let m = (l + h) / 2;
        let cell = downcast::<T>(&cells[m as usize]).unwrap();
        let payload = payload_extractor_fn(cell);
        let ordering = filter_cmp(filter_value, &payload);
        // println!("{:?} {:?} {:?}", filter_value, payload, ordering);

        match ordering {
            Ordering::Greater => {
                l = m + 1;
            }
            Ordering::Equal => {
                ret = SearchResult::ThisPage(cell.clone_cell(), m);
                h = m;
            }
            Ordering::Less => {
                ret = SearchResult::LeftPage(cell.clone_cell());
                h = m;
            }
        }
    }
    ret
}

fn get_payload_id(cell: &dyn Cell) -> (String, String) {
    let rows = cell.record().unwrap().rows;
    (rows.first().unwrap().to_string(), rows[1].clone())
}

fn fetch_table_data(
    col_positions: &[usize],
    page_num_and_page: &(u32, Page),
    filter: &Filter,
) -> anyhow::Result<Vec<String>> {
    let (_, page) = page_num_and_page;
    let page_type = page.page_header.page_type;
    let mut rows = Vec::new();
    if page_type == PageType::TblLeaf {
        page.cells.iter().for_each(|cell| {
            let cell = downcast::<TableLeafCell>(cell).unwrap();
            if let Some(row) = filter_rows(filter, cell, col_positions) {
                rows.push(row);
            }
        });
        Ok(rows)
    } else {
        bail!("type unhandled {:?}", page_type);
    }
}

fn decode_match(filter: &Filter, rows: &[String]) -> bool {
    if rows.len() <= filter.filter_col_pos as usize {
        return false;
    }

    filter_cmp(&filter.filter_value, &rows[filter.filter_col_pos as usize]) == Ordering::Equal
}

fn fetch_table_first_page(cell: &dyn Cell, builder: &mut PageReaderBuilder) -> (u32, Page) {
    /* page where the table is stored */
    let page_no: u32 = cell.record().unwrap().rows.get(3).unwrap().parse().unwrap();
    (page_no, builder.new_reader(page_no).read_page())
}

fn get_column_position(select_col_names: Vec<String>, table_columns: &[String]) -> Vec<usize> {
    let mut col_positions = Vec::new();
    if select_col_names.first().unwrap() == "*" {
        for pos in 0..table_columns.len() {
            col_positions.push(pos);
        }
    } else {
        select_col_names.iter().for_each(|col| {
            col_positions.push(
                table_columns
                    .iter()
                    .position(|name| name == col)
                    .expect("column {col} not found"),
            );
        });
    }
    col_positions
}

fn filter_cmp(filter_value: &FilterValue, payload: &String) -> Ordering {
    match filter_value {
        FilterValue::String(filter_string) => filter_string.cmp(payload),
        FilterValue::Int(filter_int) => {
            let payload_int = payload.parse::<i64>().unwrap();
            filter_int.cmp(&payload_int)
        }
    }
}
