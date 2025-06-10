use anyhow::{bail, Result};
use codecrafters_sqlite::file_reader::FileReader;
use codecrafters_sqlite::page::{downcast, Cell, IdxLeafCell, Page, TableIntCell, TableLeafCell};
use codecrafters_sqlite::page_reader::PageReaderBuilder;
use codecrafters_sqlite::page_type::PageType;
use codecrafters_sqlite::parser::{parse_sql, QueryDetails, QueryType};
use std::clone::Clone;
use std::collections::{HashMap, HashSet};
use std::iter::Iterator;
use std::ops::Deref;
use std::string::ToString;

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

    let mut db_root_page_reader = builder.new_reader(1_u16);
    // println!("Reading root page...");
    let db_root_page = db_root_page_reader.read_page();
    let create_replacement_map: HashMap<&str, &str> =
        HashMap::from([("\n", ""), ("\t", ""), ("\"", "")]);

    match command.as_str() {
        ".dbinfo" => {
            eprintln!("Logs from your program will appear here!");
            println!("database page size: {}", page_size);
            println!("number of tables: {}", db_root_page.page_header.cell_count);
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

            for cell in db_root_page.cells {
                let cell = downcast::<TableLeafCell>(&cell).unwrap();
                let table = cell.record.rows.get(2).unwrap();

                tables.push_str(table);
                tables.push(' ');

                let sql = cell
                    .record
                    .rows
                    .get(4)
                    .unwrap()
                    .replace("\n", "")
                    .replace("\t", "");
                sqls.push_str(&sql);
                // println!("{:?}", cell.record.rows.get(3));
            }

            println!("{:?}", tables.trim());
            println!("{:?}", sqls);
        }
        _ => {
            let select_query_details =
                parse_sql(command, HashMap::new()).expect("Unknown query type");
            match select_query_details.qtype {
                QueryType::SELECT(count) => {
                    let table_name = select_query_details.stmt.table_name;
                    let select_col_names = select_query_details.stmt.columns;
                    let root_page_cell =
                        fetch_cell::<TableLeafCell>(&table_name, "index", &db_root_page)
                            .unwrap()
                            .deref();

                    // println!("{:?}", select_col_names);

                    if select_col_names.len() == 1
                        && select_col_names.first().unwrap() == "*"
                        && count
                    {
                        let (page_no, page) = fetch_table_first_page(root_page_cell, &mut builder);
                        let page_num_and_page: Vec<(u32, Page)> =
                            fetch_all_leaves_for_table(page, &mut builder, page_no);
                        println!(
                            "{:?}",
                            page_num_and_page
                                .iter()
                                .map(|(_, page)| page.page_header.cell_count as u64)
                                .sum::<u64>()
                        );
                    } else {
                        let table_query_details =
                            match fetch_cell::<IdxLeafCell>(&table_name, "table", &db_root_page) {
                                Some(root_page_cell) => get_query_details::<IdxLeafCell>(
                                    root_page_cell.deref(),
                                    create_replacement_map,
                                ),
                                None => get_query_details::<TableLeafCell>(
                                    root_page_cell,
                                    create_replacement_map,
                                ),
                            };

                        println!("{:?}", table_query_details);

                        match table_query_details.qtype {
                            QueryType::CREATE => {
                                let (page_no, page) =
                                    fetch_table_first_page(root_page_cell, &mut builder);
                                let page_num_and_page: Vec<(u32, Page)> =
                                    fetch_all_leaves_for_table(page, &mut builder, page_no);
                                let col_positions =
                                    get_column_position(select_col_names, &table_query_details);
                                // println!(">>> {:?}", col_positions);
                                let filter = get_filter_col_pos(
                                    select_query_details.stmt.filter,
                                    &table_query_details,
                                );
                                // println!(">>> {:?}", filter);
                                // let mut unique_rows = Vec::new();
                                for page in &page_num_and_page {
                                    let unique_rows_sub =
                                        fetch_table_data(&col_positions, page, &filter)?;
                                    unique_rows_sub.iter().for_each(|row| {
                                        println!("{}", row);
                                    });
                                    // unique_rows.extend(unique_rows_sub);
                                }
                                // println!("Found {:?}", unique_rows.len());
                            }
                            QueryType::INDEX(idx_name) => {
                                println!("{:?}", idx_name);
                                let (page_no, page) =
                                    fetch_table_first_page(root_page_cell, &mut builder);
                                println!("{:?}", page);
                            }
                            _ => {
                                bail!("Invalid data read");
                            }
                        }
                    }
                }
                _ => {
                    bail!("Missing or invalid command passed: {}", command)
                }
            }
        }
    }

    Ok(())
}

fn fetch_table_first_page(cell: &dyn Cell, builder: &mut PageReaderBuilder) -> (u32, Page) {
    /* page where the table is stored */
    let page_no: u8 = cell.record().unwrap().rows.get(3).unwrap().parse().unwrap();
    (
        page_no as u32,
        builder.new_reader(page_no as u16).read_page(),
    )
}

fn fetch_all_leaves_for_table(
    first_page: Page,
    builder: &mut PageReaderBuilder,
    page_no: u32,
) -> Vec<(u32, Page)> {
    if first_page.page_header.page_type == PageType::TblLeaf {
        vec![(page_no, first_page.clone())]
    } else if first_page.page_header.page_type == PageType::TblInt {
        // look for table leaves
        fetch_all_leaves(first_page, builder, page_no)
    } else {
        Vec::new()
    }
}

fn fetch_cell<'a, T: Cell>(
    table_name: &str,
    schema_type: &str,
    page: &'a Page,
) -> Option<&'a Box<dyn Cell>> {
    // println!("fetching cell idx for {page}");
    let cell_idx = page
        .cells
        .iter()
        .position(|cell| match downcast::<T>(cell) {
            Some(cell) => {
                let rows = cell.record().unwrap().rows;
                rows.get(2).unwrap() == table_name && rows.first().unwrap() == schema_type
            }
            None => false,
        });
    match cell_idx {
        Some(idx) => Some(&page.cells[idx]),
        None => None,
    }
}

fn get_query_details<T: Cell>(
    cell: &dyn Cell,
    create_replacement_map: HashMap<&str, &str>,
) -> QueryDetails {
    parse_sql(&cell.record().unwrap().rows[4], create_replacement_map).unwrap()
}

fn get_filter_col_pos(
    filter: Option<(String, String)>,
    create_query_details: &QueryDetails,
) -> Filter {
    if filter.is_some() {
        let filter = filter.clone().unwrap();
        Filter {
            filter_col_pos: create_query_details
                .stmt
                .columns
                .iter()
                .position(|name| *name == filter.0)
                .unwrap() as isize,
            filter_value: filter.1,
        }
    } else {
        Filter {
            filter_col_pos: -1,
            filter_value: String::new(),
        }
    }
}

fn get_column_position(
    select_col_names: Vec<String>,
    create_query_details: &QueryDetails,
) -> Vec<usize> {
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

fn fetch_table_data(
    col_positions: &[usize],
    page_num_and_page: &(u32, Page),
    filter: &Filter,
) -> Result<Vec<String>> {
    let (page_no, page) = page_num_and_page;
    let page_type = page.page_header.page_type;
    let mut rows = Vec::new();
    if page_type == PageType::TblLeaf {
        page.cells.iter().for_each(|cell| {
            let cell = downcast::<TableLeafCell>(cell).unwrap();
            // println!(">>> {:?}", cell);
            // println!(">>> {:?}", page_no);
            if let Some(row) = filter_rows(filter, cell, col_positions) {
                rows.push(row);
            }
        });
        // println!(">>> {:?}, {}", &page.page_header, page_no);
        Ok(rows)
    } else {
        bail!("type unhandled {:?}", page_type);
    }
}

fn filter_rows(filter: &Filter, cell: &TableLeafCell, col_positions: &[usize]) -> Option<String> {
    let rows = &cell.record.rows;
    let mut row_str = Vec::new();

    if col_positions.len() > rows.len() {
        rows.iter().for_each(|row| row_str.push(row.clone()));
        return Some(row_str.join("|"));
    }

    if filter.filter_col_pos == -1 || decode_match(filter, rows) {
        col_positions.iter().for_each(|&pos| {
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

fn fetch_all_leaves(
    first_page: Page,
    builder: &mut PageReaderBuilder,
    first_page_no: u32,
) -> Vec<(u32, Page)> {
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
        if let Some(right_page_no) = int_page.page_header.right_pointer {
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
        int_page.cells.iter().for_each(|cell| {
            let cell = downcast::<TableIntCell>(cell).unwrap();
            let left_page_no = cell.left_child_page_no;
            // print!("{} <-- {:?}: {}, ", left_page_no, page_no, cell.row_id);
            if !visited.contains(&left_page_no) {
                let mut reader = builder.new_reader(left_page_no as u16);
                if reader.page_meta_data.page_type == PageType::TblLeaf
                    || reader.page_meta_data.page_type == PageType::TblInt
                {
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

fn decode_match(filter: &Filter, rows: &[String]) -> bool {
    if rows.len() <= filter.filter_col_pos as usize {
        return false;
    }

    // println!("{:?} {:?}", val, filter.filter_value);
    rows[filter.filter_col_pos as usize] == filter.filter_value
}
