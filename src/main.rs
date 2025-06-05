use anyhow::{bail, Result};
use codecrafters_sqlite::cell::{Cell, TableLeafCell};
use codecrafters_sqlite::file_reader::FileReader;
use codecrafters_sqlite::page::PageReader;
use codecrafters_sqlite::parser::{parse_sql, QueryType};
use std::collections::HashMap;
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
    let mut header_reader = file_reader.read_bytes(18).unwrap();
    let header = header_reader.from_offset(16, 2).unwrap();
    let page_size = u16::from_be_bytes([header[0], header[1]]);
    let mut page_reader = PageReader::new(&mut file_reader, 1_u16, page_size);
    let root_page = page_reader.read_page();

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
            }

            println!("{:?}", tables.trim());
            println!("{:?}", sqls);
        }
        _ => {
            let query_details = parse_sql(command, HashMap::new()).expect("Unknown query type");
            match query_details.qtype {
                QueryType::SELECT => {
                    let table_name = query_details.stmt.table_name;
                    let cell_idx = root_page
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
                    let cell = &root_page.cells[cell_idx];
                    let cell = cell
                        .deref()
                        .as_any()
                        .downcast_ref::<TableLeafCell>()
                        .unwrap();
                    /* page where the table is stored */
                    let page_no_bytes = cell.record.rows.get(3).unwrap();
                    let page_no = u8::from_be_bytes([page_no_bytes[0]]);

                    let col_names = query_details.stmt.columns;
                    //println!("{:?}", col_names);
                    let page =
                        PageReader::new(&mut file_reader, page_no as u16, page_size).read_page();
                    if col_names.len() == 1 && col_names.first().unwrap() == "*" {
                        println!("{}", page.page_header.cell_count);
                    } else {
                        let create_replacement_map = HashMap::from([("\n", ""), ("\t", "")]);
                        let create_table_sql = String::from_utf8_lossy(&cell.record.rows[4]);
                        //println!("{}", create_table_sql);

                        let create_query_details =
                            parse_sql(&create_table_sql, create_replacement_map).unwrap();
                        //println!("{:?}", create_query_details.stmt.columns);

                        match create_query_details.qtype {
                            QueryType::CREATE => {
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

                                let filter = query_details.stmt.filter;
                                //println!("{:?}", filter);
                                let (filter_col_pos, filter_value) = if filter.is_some() {
                                    let filter = filter.clone().unwrap();
                                    (
                                        create_query_details
                                            .stmt
                                            .columns
                                            .iter()
                                            .position(|name| *name == filter.0)
                                            .unwrap()
                                            as isize,
                                        filter.1,
                                    )
                                } else {
                                    (-1, String::new())
                                };

                                page.cells.iter().for_each(|cell| {
                                    let cell = cell
                                        .deref()
                                        .as_any()
                                        .downcast_ref::<TableLeafCell>()
                                        .unwrap();
                                    if should_use(filter_col_pos, &filter_value, &cell.record.rows)
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

fn should_use(filter_col_pos: isize, filter_value: &str, rows: &[Box<[u8]>]) -> bool {
    if filter_col_pos == -1 {
        true
    } else {
        String::from_utf8_lossy(&rows[filter_col_pos as usize]) == filter_value
    }
}
