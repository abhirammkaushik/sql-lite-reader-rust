use std::char;

use anyhow::{bail, Result};
use codecrafters_sqlite::file_reader::FileReader;
use codecrafters_sqlite::page::PageReader;

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
            let mut tables = String::new();
            for cell in root_page.cells {
                for ch in cell.record.rows.get(2).unwrap() {
                    tables.push(char::from_u32(*ch as u32).unwrap());
                }

                tables.push(' ');
            }

            println!("{:?}", tables.trim());
        }
        _ => {
            let mut sql = command.split(' ');
            match sql.next().unwrap() {
                "SELECT" | "select" => {
                    if sql.next().unwrap().to_lowercase() != "count(*)" {
                        bail!("not implemented");
                    }

                    let table_name = sql.next_back().unwrap();
                    let cell = root_page
                        .cells
                        .iter()
                        .find(|cell| {
                            String::from_utf8_lossy(cell.record.rows.get(2).unwrap()) == table_name
                        })
                        .unwrap();
                    /* page where the table is stored */
                    let page_no_bytes = cell.record.rows.get(3).unwrap();
                    let page_no = u8::from_be_bytes([page_no_bytes[0]]);
                    //println!("{} found in page: {}", table_name, page_no);
                    let page_2 =
                        PageReader::new(&mut file_reader, page_no as u16, page_size).read_page();
                    println!("{}", page_2.page_header.cell_count);
                }
                _ => {
                    bail!("Missing or invalid command passed: {}", command)
                }
            }
        }
    }

    Ok(())
}
