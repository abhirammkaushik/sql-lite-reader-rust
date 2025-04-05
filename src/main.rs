use anyhow::{bail, Result};
use codecrafters_sqlite::file_reader::FileReader;
use codecrafters_sqlite::page::PageReader;
use codecrafters_sqlite::parser::{parse_sql, QueryType};

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
            //let mut sql = command.split(' ');
            let command = command.replace(" ", "");
            let query_details = parse_sql(&command).expect("Unknown query type");
            match query_details.qtype {
                QueryType::SELECT => {
                    let table_name = query_details.stmt.table_name;
                    let cell_idx = root_page
                        .cells
                        .iter()
                        .position(|cell| {
                            String::from_utf8_lossy(cell.record.rows.get(2).unwrap()) == table_name
                        })
                        .expect("table not found");
                    let cell = &root_page.cells[cell_idx];
                    /* page where the table is stored */
                    let page_no_bytes = cell.record.rows.get(3).unwrap();
                    let page_no = u8::from_be_bytes([page_no_bytes[0]]);

                    let col_names = query_details.stmt.columns;
                    //println!("{:?}", col_names);
                    let page =
                        PageReader::new(&mut file_reader, page_no as u16, page_size).read_page();
                    if col_names.len() == 1 && col_names.first().unwrap() == "*" {
                        //println!("{} found in page: {}", table_name, page_no);
                        println!("{}", page.page_header.cell_count);
                    } else {
                        let create_table_sql = String::from_utf8_lossy(&cell.record.rows[4])
                            .replace("\n", "")
                            .replace("\t", "");
                        println!("{}", create_table_sql);

                        let create_query_details = parse_sql(&create_table_sql).unwrap();
                        println!("{:?}", create_query_details.stmt.columns);
                        match create_query_details.qtype {
                            QueryType::CREATE => {
                                let mut col_positions = Vec::new();
                                create_query_details
                                    .stmt
                                    .columns
                                    .iter()
                                    .enumerate()
                                    .for_each(|(idx, col)| {
                                        if col_names.contains(col) {
                                            col_positions.push(idx)
                                        }
                                    });

                                if col_positions.len() != col_names.len() {
                                    bail!(
                                        "some of the columns {:?} not found in table '{}'.",
                                        col_names,
                                        table_name
                                    )
                                }

                                page.cells.iter().for_each(|cell| {
                                    let mut row = Vec::new();
                                    col_positions.iter().for_each(|&pos| {
                                        row.push(String::from_utf8_lossy(&cell.record.rows[pos]))
                                    });

                                    println!("{}", row.join("|"))
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
