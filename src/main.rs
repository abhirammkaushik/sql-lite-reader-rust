use anyhow::{bail, Result};
use codecrafters_sqlite::cell;
use codecrafters_sqlite::cell::{Cell, TableIntCell, TableLeafCell};
use codecrafters_sqlite::file_reader::FileReader;
use codecrafters_sqlite::page::{Page, PageReaderBuilder};
use codecrafters_sqlite::page_type::PageType;
use codecrafters_sqlite::parser::{parse_sql, QueryDetails, QueryType};
use std::collections::HashMap;
use std::ops::Deref;

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
    let mut header_reader = file_reader.read_bytes(18).unwrap();
    let header = header_reader.from_offset(16, 2).unwrap();
    let page_size = u16::from_be_bytes([header[0], header[1]]);
    let mut builder = PageReaderBuilder::new(file_reader, page_size);
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
                let cell = cell
                    .deref()
                    .as_any()
                    .downcast_ref::<TableLeafCell>()
                    .unwrap();
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
                QueryType::SELECT => {
                    let table_name = query_details.stmt.table_name;
                    let col_names = query_details.stmt.columns;
                    // println!("{:?}", col_names);
                    let page = fetch_table_first_page(table_name.as_str(), &root_page, &mut builder);
                    let pages: Vec<&Page> = fetch_all_leaves_for_table(table_name.as_str(), &page, &mut builder);

                    if col_names.len() == 1 && col_names.first().unwrap() == "*" {
                        println!("{}", page.page_header.cell_count);
                    } else {
                        let create_query_details =
                            get_create_table_query_details(table_name.as_str(), &root_page);
                        //println!("{:?}", create_query_details.stmt.columns);

                        match create_query_details.qtype {
                            QueryType::CREATE => {
                                let col_positions = get_column_position(col_names, &create_query_details);
                                let filter = get_filter_col_pos(query_details.stmt.filter, &create_query_details);
                                //println!("{:?}", filter);
                                fetch_table_data(col_positions, &page, filter)?;
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

fn fetch_all_leaves_for_table<'a>(table_name: &str, first_page: &'a Page, builder: &mut PageReaderBuilder) -> Vec<&'a Page> {
    let mut pages = vec![];
    if first_page.page_header.page_type == PageType::TblLeaf {
        pages.push(first_page);
    } else if first_page.page_header.page_type == PageType::TblInt {
        // look for table leaves
    }
    pages
}

fn fetch_table_first_page(table_name: &str, parent_page: &Page, builder: &mut PageReaderBuilder) -> Page {
    let cell_idx = fetch_table_leaf_cell_idx(table_name, parent_page);
    let cell = &parent_page.cells[cell_idx];
    let cell = cell
        .deref()
        .as_any()
        .downcast_ref::<TableLeafCell>()
        .unwrap();
    /* page where the table is stored */
    let page_no_bytes = cell.record.rows.get(3).unwrap();
    let page_no = u8::from_be_bytes([page_no_bytes[0]]);
    builder.new_reader(page_no as u16).read_page()
}

fn fetch_table_leaf_cell_idx(table_name: &str, page: &Page) -> usize {
    let cell_idx = page
        .cells
        .iter()
        .position(|cell| {
            let cell = cell
                .deref()
                .as_any()
                .downcast_ref::<TableLeafCell>()
                .unwrap();
            String::from_utf8_lossy(cell.record.rows.get(2).unwrap()) == table_name
        })
        .expect("table not found");
    cell_idx
}

fn get_create_table_query_details(table_name: &str, parent_page: &Page) -> QueryDetails {
    let create_replacement_map = HashMap::from([("\n", ""), ("\t", ""), ("\"", "")]);
    let cell_idx = fetch_table_leaf_cell_idx(table_name, parent_page);
    let cell = parent_page.cells[cell_idx].deref().as_any().downcast_ref::<TableLeafCell>().unwrap();
    let sql = String::from_utf8_lossy(&cell.record.rows[4]).to_string();
    //println!("{}", create_table_sql);
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

fn get_column_position(col_names: Vec<String>, create_query_details: &QueryDetails) -> Vec<usize> {
    let mut col_positions = Vec::new();
    col_names.iter().for_each(|col| {
        col_positions.push(
            create_query_details
                .stmt
                .columns
                .iter()
                .position(|name| name == col)
                .expect("column {col} not found"),
        );
    });
    col_positions
}

fn fetch_table_data(col_positions: Vec<usize>, page: &Page, filter: Filter) -> Result<()> {
    let page_type = page.page_header.page_type;
    if page_type == PageType::TblLeaf {
        page.cells.iter().for_each(|cell| {
            let cell = cell::downcast::<TableLeafCell>(cell);
            if should_use(&filter, &cell.record.rows)
            {
                let mut row = Vec::new();
                col_positions.iter().for_each(|&pos| {
                    row.push(String::from_utf8_lossy(
                        &cell.record.rows[pos],
                    ))
                });

                println!("{}", row.join("|"))
            }
        });
    } else if page_type == PageType::TblInt {
        page.cells.iter().for_each(|cell| {
            let cell = cell::downcast::<TableIntCell>(cell);
            println!("{}, {}", cell.row_id, cell.left_child_page_id)
        });
    } else {
        bail!("type unhandled {:?}", page_type);
    }
    Ok(())
}

fn should_use(filter: &Filter, rows: &[Box<[u8]>]) -> bool {
    if filter.filter_col_pos == -1 {
        true
    } else {
        String::from_utf8_lossy(&rows[filter.filter_col_pos as usize]) == filter.filter_value
    }
}

