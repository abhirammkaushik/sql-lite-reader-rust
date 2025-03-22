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

    match command.as_str() {
        ".dbinfo" => {
            eprintln!("Logs from your program will appear here!");
            println!("database page size: {}", page_size);

            let mut page_reader = PageReader::new(&mut file_reader, 1_u16, page_size);
            let page = page_reader.read_page();

            println!("number of tables: {}", page.page_header.cell_count);
        }
        ".tables" => {
            let mut file_reader = FileReader::new(path).unwrap();
            let mut page_reader = PageReader::new(&mut file_reader, 1_u16, page_size);
            let page = page_reader.read_page();

            let mut tables = String::new();
            for cell in page.cells {
                for ch in cell.record.rows.get(2).unwrap() {
                    tables.push(char::from_u32(*ch as u32).unwrap());
                }

                tables.push(' ');
            }

            println!("{:?}", tables.trim());
        }

        _ => bail!("Missing or invalid command passed: {}", command),
    }

    Ok(())
}
