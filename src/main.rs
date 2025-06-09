use anyhow::{bail, Result};
use codecrafters_sqlite::file_reader::FileReader;
use codecrafters_sqlite::page::{downcast, Page, RecordHeader, SerialType, TableIntCell, TableLeafCell};
use codecrafters_sqlite::page_reader::PageReaderBuilder;
use codecrafters_sqlite::page_type::PageType;
use codecrafters_sqlite::parser::{parse_sql, QueryDetails, QueryType};
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Filter {
    filter_col_pos: isize,
    filter_value: String,
}

fn main() -> Result<()> {
    // Parse arguments
    let args = std::env::args().collect::<Vec<_>>();
    match args.len() {
        0 | 1 => bail!("Missing <database path> and <command>"),
        2 => bail!("Missing <command>"),
        _ => {}
    }

    // Parse command and act accordingly
    let command = &args[2];
    let path = &args[1];
    let mut file_reader = FileReader::new(path).unwrap();
    let mut header_reader = file_reader.read_bytes(18)?;
    let header = header_reader.from_offset(16, 2).unwrap();
    let page_size = u16::from_be_bytes([header[0], header[1]]);
    // let mut free_list_page_iter = file_reader.read_bytes_from(36, 8)?;
    let mut builder = PageReaderBuilder::new(file_reader, page_size);

    // let ffltp_bytes = free_list_page_iter.next_n(4).unwrap(); // first free list trunk page
    // let ffltp_bytes: [u8; 4] = ffltp_bytes.as_ref().try_into()?;
    // let free_list_trunk_page_no = u32::from_be_bytes(ffltp_bytes);
    // println!("free_list_trunk_page: {}", free_list_trunk_page_no);
    // // let mut free_list_pages = Vec::new();
    // if free_list_trunk_page_no != 0 {
    //     let free_list_pages_count_bytes: [u8; 4] = free_list_page_iter.next_n(4).unwrap().as_ref().try_into()?;
    //     let free_list_pages_count = u32::from_be_bytes(free_list_pages_count_bytes);
    //     let offset = page_size as u64 * (free_list_trunk_page_no as u64 - 1);
    //     // let mut iterator = file_reader.read_bytes_from(offset, page_size as usize)?;
    //     // println!("reading page {free_list_trunk_page_no} at offset {offset}");
    //     // let bytes = iterator.next_n(page_size as usize).unwrap();
    //     // println!("{:?}", bytes);
    //     let mut reader = builder.new_reader(free_list_trunk_page_no as u16);
    //     // let ffltp_page = reader.read_page();
    //     println!("total free list pages: {}", free_list_pages_count);
    //     // println!("free_list_trunk_page: {:?}", ffltp_page);
    // }
    let mut root_page_reader = builder.new_reader(1_u16);
    // println!("Reading root page...");
    let root_page = root_page_reader.read_page();

    match command.as_str() {
        ".dbinfo" => {
            eprintln!("Logs from your program will appear here!");
            println!("database page size: {}", page_size);
            println!("number of tables: {}", root_page.page_header.cell_count);
        }
        ".tables" => {
            /* index | info
                  0 | type
                  1 | name
                  2 | table_name
                  3 | page where table data is stored
                  4 | table creation sql
            */

            let mut tables = String::new();
            let mut sqls = String::new();

            for cell in root_page.cells {
                let cell = downcast::<TableLeafCell>(&cell);
                let table = String::from_utf8_lossy(cell.record.rows.get(2).unwrap());

                tables.push_str(&table);
                tables.push(' ');

                let sql = String::from_utf8_lossy(cell.record.rows.get(4).unwrap())
                    .replace("\n", "")
                    .replace("\t", "");
                sqls.push_str(&sql);
                // println!("{:?}", cell.record.rows.get(3));
            }

            println!("{:?}", tables.trim());
            println!("{:?}", sqls);
        }
        _ => {
            let query_details = parse_sql(command, HashMap::new()).expect("Unknown query type");
            match query_details.qtype {
                QueryType::SELECT(count) => {
                    let table_name = query_details.stmt.table_name;
                    let select_col_names = query_details.stmt.columns;
                    // println!("{:?}", select_col_names);
                    let (page_no, page) = fetch_table_first_page(table_name.as_str(), &root_page, &mut builder);
                    // println!("{:?}", page.page_header);
                    let page_num_and_page: Vec<(u32, Page)> = fetch_all_leaves_for_table(page, &mut builder, page_no);
                    // println!("Found {:?}", page_num_and_page.iter().map(|(num, _)| num).collect::<Vec<_>>());

                    if select_col_names.len() == 1 && select_col_names.first().unwrap() == "*" && count {
                        println!("{:?}", page_num_and_page.iter().map(|(_, page)| page.page_header.cell_count as u64).sum::<u64>());
                    } else {
                        let create_query_details =
                            get_create_table_query_details(table_name.as_str(), &root_page);
                        // println!("{:?}", create_query_details);

                        match create_query_details.qtype {
                            QueryType::CREATE => {
                                let col_positions = get_column_position(select_col_names, &create_query_details);
                                // println!(">>> {:?}", col_positions);
                                let filter = get_filter_col_pos(query_details.stmt.filter, &create_query_details);
                                // println!(">>> {:?}", filter);
                                // let mut unique_rows = Vec::new();
                                for page in &page_num_and_page {
                                    let unique_rows_sub = fetch_table_data(&col_positions, page, &filter)?;
                                    unique_rows_sub.iter().for_each(|row| {
                                        println!("{}", row);
                                    });
                                    // unique_rows.extend(unique_rows_sub);
                                }
                                // println!("Found {:?}", unique_rows.len());
                            }
                            _ => {
                                bail!("Invalid data read");
                            }
                        }
                    }
                }
                QueryType::CREATE => {
                    bail!("Missing or invalid command passed: {}", command)
                }
            }
        }
    }

    Ok(())
}

fn fetch_table_first_page(table_name: &str, parent_page: &Page, builder: &mut PageReaderBuilder) -> (u32, Page) {
    let cell_idx = fetch_table_leaf_cell_idx(table_name, parent_page);
    let cell = &parent_page.cells[cell_idx];
    let cell = downcast::<TableLeafCell>(cell);
    /* page where the table is stored */
    let page_no_bytes = cell.record.rows.get(3).unwrap();
    let page_no = u8::from_be_bytes([page_no_bytes[0]]);
    (page_no as u32, builder.new_reader(page_no as u16).read_page())
}

fn fetch_all_leaves_for_table(first_page: Page, builder: &mut PageReaderBuilder, page_no: u32) -> Vec<(u32, Page)> {
    if first_page.page_header.page_type == PageType::TblLeaf {
        vec![(page_no, first_page.clone())]
    } else if first_page.page_header.page_type == PageType::TblInt {
        // look for table leaves
        fetch_all_leaves(first_page, builder, page_no)
    } else {
        Vec::new()
    }
}

fn fetch_table_leaf_cell_idx(table_name: &str, page: &Page) -> usize {
    let cell_idx = page
        .cells
        .iter()
        .position(|cell| {
            let cell = downcast::<TableLeafCell>(cell);
            String::from_utf8_lossy(cell.record.rows.get(2).unwrap()) == table_name
        })
        .expect("table not found");
    cell_idx
}

fn get_create_table_query_details(table_name: &str, parent_page: &Page) -> QueryDetails {
    let create_replacement_map = HashMap::from([("\n", ""), ("\t", ""), ("\"", "")]);
    let cell_idx = fetch_table_leaf_cell_idx(table_name, parent_page);
    let cell = downcast::<TableLeafCell>(&parent_page.cells[cell_idx]);
    let sql = String::from_utf8_lossy(&cell.record.rows[4]).to_string();
    // println!(">>> {}", sql);
    parse_sql(&sql, create_replacement_map).unwrap()
}

fn get_filter_col_pos(filter: Option<(String, String)>, create_query_details: &QueryDetails) -> Filter {
    if filter.is_some() {
        let filter = filter.clone().unwrap();
        Filter {
            filter_col_pos: create_query_details
                .stmt
                .columns
                .iter()
                .position(|name| *name == filter.0)
                .unwrap()
                as isize,
            filter_value: filter.1,
        }
    } else {
        Filter {
            filter_col_pos: -1,
            filter_value: String::new(),
        }
    }
}

fn get_column_position(select_col_names: Vec<String>, create_query_details: &QueryDetails) -> Vec<usize> {
    let mut col_positions = Vec::new();
    if select_col_names.first().unwrap() == "*" {
        for pos in 0..create_query_details.stmt.columns.len() {
            col_positions.push(pos);
        }
    } else {
        select_col_names.iter().for_each(|col| {
            col_positions.push(
                create_query_details
                    .stmt
                    .columns
                    .iter()
                    .position(|name| name == col)
                    .expect("column {col} not found"),
            );
        });
    }
    col_positions
}

fn fetch_table_data(col_positions: &Vec<usize>, page_num_and_page: &(u32, Page), filter: &Filter) -> Result<Vec<String>> {
    let (page_no, page) = page_num_and_page;
    let page_type = page.page_header.page_type;
    let mut rows = Vec::new();
    if page_type == PageType::TblLeaf {
        page.cells.iter().for_each(|cell| {
            let cell = downcast::<TableLeafCell>(cell);
            // println!(">>> {:?}", cell);
            // println!(">>> {:?}", page_no);
            match filter_or_get(&filter, cell, col_positions) {
                Some(row) => {
                    rows.push(row);
                }
                None => {}
            }
        });
        // println!(">>> {:?}, {}", &page.page_header, page_no);
        Ok(rows)
    } else {
        bail!("type unhandled {:?}", page_type);
    }
}

fn filter_or_get(filter: &Filter, cell: &TableLeafCell, col_positions: &Vec<usize>) -> Option<String> {
    let rows = &cell.record.rows;
    let record_header = &cell.record.record_header;
    let mut row_str = Vec::new();

    let decode = |pos: usize, row| {
        match record_header.serial_types[pos] {
            SerialType::INTEGER(size) => {
                row_u64_converter(row, size).to_string()
            }
            SerialType::TEXT(_size) | SerialType::BLOB(_size) => {
                String::from_utf8_lossy(row).to_string()
            }
            SerialType::FLOAT64(_size) => {
                f64::from_be_bytes([row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7]]).to_string()
            }
            _ =>
                String::new()
        }
    };

    if col_positions.len() > rows.len() {
        rows.iter().enumerate().for_each(|(idx, row)| row_str.push(decode(idx, row)));
        return Some(row_str.join("|"));
    }

    if filter.filter_col_pos == -1 || decode_match(filter, rows, record_header) {
        col_positions.iter().for_each(|&pos| {
            // println!("{:?}", &rows);
            // println!("{:?}", record_header);
            let row = &rows[pos];
            // println!("{:?}", row);
            // println!("{:?}", record_header.serial_types[pos]);
            let val = if pos == 0 {
                cell.row_id.to_string()
            } else {
                decode(pos, row)
            };
            row_str.push(val);
        });
        return Some(row_str.join("|"));
    }
    None
}

fn fetch_all_leaves(first_page: Page, builder: &mut PageReaderBuilder, first_page_no: u32) -> Vec<(u32, Page)> {
    let mut pages = vec![];
    let mut stack = std::collections::VecDeque::new();
    let mut visited = HashSet::new();
    // let mut debug_pages = Vec::new();
    stack.push_back((first_page_no, first_page));
    while !stack.is_empty() {
        let (page_no, int_page) = stack.pop_front().unwrap();
        // println!(">>> {:?}, {}", &int_page.page_header, page_no);
        // if page_no == 2 || page_no == 50 {
        //     debug_pages.push(int_page.clone());
        // }
        match int_page.page_header.right_pointer {
            Some(right_page_no) => {
                // print!("{} --> {:?}, ", page_no, right_page_no);
                if !visited.contains(&right_page_no) {
                    visited.insert(right_page_no);
                    let right_page = builder.new_reader(right_page_no as u16).read_page();
                    if right_page.page_header.page_type == PageType::TblLeaf {
                        pages.push((right_page_no, right_page));
                    } else if right_page.page_header.page_type == PageType::TblInt {
                        stack.push_back((right_page_no, right_page));
                    }
                }
            }
            _ => {}
        }
        int_page.cells.iter().for_each(|cell| {
            let cell = downcast::<TableIntCell>(cell);
            let left_page_no = cell.left_child_page_no;
            // print!("{} <-- {:?}: {}, ", left_page_no, page_no, cell.row_id);
            if !visited.contains(&left_page_no) {
                let mut reader = builder.new_reader(left_page_no as u16);
                if reader.page_meta_data.page_type == PageType::TblLeaf || reader.page_meta_data.page_type == PageType::TblInt {
                    let left_page = reader.read_page();
                    if left_page.page_header.page_type == PageType::TblLeaf {
                        pages.push((left_page_no, left_page));
                    } else {
                        stack.push_back((left_page_no, left_page));
                    }
                }
                visited.insert(left_page_no);
            }
        });
        println!();
    }
    // debug_pages.iter().for_each(|page| { println!("{:?}", page); });
    pages
}

fn decode_match(filter: &Filter, rows: &[Box<[u8]>], record_header: &RecordHeader) -> bool {
    if rows.len() <= filter.filter_col_pos as usize {
        return false;
    }

    let decoder = &record_header.serial_types[filter.filter_col_pos as usize];
    let val = match decoder {
        SerialType::INTEGER(size) => row_u64_converter(&rows[filter.filter_col_pos as usize], *size).to_string(),
        SerialType::TEXT(_) => String::from_utf8_lossy(&rows[filter.filter_col_pos as usize]).to_string(),
        _ => String::new()
    };
    // println!("{:?} {:?}", val, filter.filter_value);
    val == filter.filter_value
}

fn row_u64_converter(row: &Box<[u8]>, n: u64) -> u64 {
    match n {
        1 => u8::from_be_bytes([row[0]]) as u64,
        2 => u16::from_be_bytes([row[0], row[1]]) as u64,
        3 => u32::from_be_bytes([0_u8, row[0], row[1], row[2]]) as u64,
        4 => u32::from_be_bytes([row[0], row[1], row[2], row[3]]) as u64,
        6 => u64::from_be_bytes([0_u8, 0_u8, row[0], row[1], row[2], row[3], row[4], row[5]]),
        8 => u64::from_be_bytes([row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7]]),
        _ => { 0_u64 }
    }
}