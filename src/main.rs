use anyhow::{bail, Result};
use codecrafters_sqlite::data_filter_processor;
use codecrafters_sqlite::data_filter_processor::{Filter, FilterValue};
use codecrafters_sqlite::file_reader::FileReader;
use codecrafters_sqlite::page::{downcast, Cell, Page, TableLeafCell};
use codecrafters_sqlite::page_reader::PageReaderBuilder;
use codecrafters_sqlite::parser::{parse_sql, QueryDetails, QueryType};
use std::clone::Clone;
use std::collections::HashMap;
use std::iter::Iterator;
use std::ops::Deref;

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
    let mut builder = PageReaderBuilder::new(file_reader, page_size);

    let mut db_root_page_reader = builder.new_reader(1_u32);
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
            }

            println!("{:?}", tables.trim());
            println!("{:?}", sqls);
        }
        _ => {
            let select_query_details =
                parse_sql(command, &HashMap::new()).expect("Unknown query type");
            match select_query_details.qtype {
                QueryType::SELECT(count) => {
                    let table_name = select_query_details.stmt.table_name;
                    let select_col_names = select_query_details.stmt.columns;
                    let root_leaf_page_cell =
                        fetch_cell(&table_name, "table", &db_root_page).expect("Table not found");

                    let create_table_details =
                        get_query_details(root_leaf_page_cell, &create_replacement_map);

                    if select_query_details
                        .stmt
                        .is_star
                        .expect("select query expects a boolean")
                        && count
                    {
                        data_filter_processor::count_all_rows(root_leaf_page_cell, &mut builder);
                    } else {
                        let root_index_page_cell = fetch_cell(&table_name, "index", &db_root_page);
                        let filter = get_filter_col_pos(
                            select_query_details.stmt.filter,
                            &create_table_details,
                        );
                        match root_index_page_cell {
                            Some(root_index_page_cell) => {
                                data_filter_processor::perform_index_scan(
                                    root_index_page_cell,
                                    root_leaf_page_cell,
                                    &mut builder,
                                    select_col_names,
                                    create_table_details,
                                    &filter,
                                );
                            }
                            None => {
                                data_filter_processor::perform_full_table_scan(
                                    root_leaf_page_cell,
                                    &mut builder,
                                    select_col_names,
                                    create_table_details,
                                    filter,
                                );
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

fn fetch_cell<'a>(table_name: &str, schema_type: &str, page: &'a Page) -> Option<&'a dyn Cell> {
    let cell = page.cells.iter().find(|cell| {
        let rows = cell.record().unwrap().rows;
        rows.get(2).unwrap() == table_name && rows.first().unwrap() == schema_type
    });
    match cell {
        Some(cell) => Some(cell.deref()),
        None => None,
    }
}

fn get_query_details(
    cell: &dyn Cell,
    create_replacement_map: &HashMap<&str, &str>,
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
            filter_value: FilterValue::String(filter.1),
        }
    } else {
        Filter {
            filter_col_pos: -1,
            filter_value: FilterValue::String(String::new()),
        }
    }
}
