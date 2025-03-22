use std::borrow::Borrow;
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
    match command.as_str() {
        ".dbinfo" => {
            eprintln!("Logs from your program will appear here!");

            let mut file_reader = FileReader::new(path).unwrap();
            let mut header_reader = file_reader.read_bytes(18).unwrap();
            let header = header_reader.from_offset(16, 2).unwrap();
            let page_size = u16::from_be_bytes([header[0], header[1]]);
            println!("database page size: {}", page_size);

            let mut page_reader = PageReader::new(&mut file_reader, 1_u16);
            //    .unwrap()
            //    .read_bytes_from(100, 8)
            //    .unwrap();
            ////let mut a = PageReader::new(path).unwrap().read_page(1);
            //let header = a.next_n(5).unwrap();
            //let page_size = u16::from_be_bytes([header[0], header[1]]);
            //println!("{:?}, {page_size}, {:?}, {:?}", a, header[0], header[1]);

            //let mut page_reader = PageReader::new(&args[1]).unwrap();
            let page = page_reader.read_page();

            println!("number of tables: {}", page.page_header.cell_count);
            //let cells = page.cells.len();
            //println!("number of cells {cells}");
            //for cell in page.cells {
            //    if cell.record_size == 0 {
            //        continue;
            //    }
            //
            //    print!(
            //        "{:?}",
            //        String::from_utf8_lossy(cell.record.rows.get(2).unwrap())
            //    );
            //
            //    //for row in cell.record.rows {
            //    //    let val = String::from_utf8_lossy(row.borrow());
            //    //    println!("{val}");
            //    //}
            //}

            //match FileReader::new(&args[1]) {
            //    Some(mut reader) => {
            //        let mut byte_iterator = reader.read_bytes(108)?;
            //        let header = byte_iterator.from_offset(16, 2).unwrap();
            //        let page_size = u16::from_be_bytes([header[0], header[1]]);
            //        println!("database page size: {}", page_size);
            //
            //        let page_header = byte_iterator.from_offset(100, 5).unwrap();
            //        let num_tables = u16::from_be_bytes([page_header[3], page_header[4]]);
            //        println!("number of tables: {}", num_tables);
            //
            //        let page_type_u8 = u8::from_be_bytes([page_header[0]]);
            //        let page_type = get_page_type(&page_type_u8);
            //        println!("Page type {:?}", page_type);
            //    }
            //    _ => bail!("unable to open file {}", &args[1]),
            //}
        }
        ".tables" => {
            let mut file_reader = FileReader::new(path).unwrap();
            let mut page_reader = PageReader::new(&mut file_reader, 1_u16);
            //    .unwrap()
            //    .read_bytes_from(100, 8)
            //    .unwrap();
            ////let mut a = PageReader::new(path).unwrap().read_page(1);
            //let header = a.next_n(5).unwrap();
            //let page_size = u16::from_be_bytes([header[0], header[1]]);
            //println!("{:?}, {page_size}, {:?}, {:?}", a, header[0], header[1]);

            //let mut page_reader = PageReader::new(&args[1]).unwrap();
            let page = page_reader.read_page();

            //println!("number of tables: {}", page.page_header.cell_count);
            //let cells = page.cells.len();
            //println!("number of cells {cells}");

            let mut tables = String::new();
            for cell in page.cells {
                //if cell.record_size == 0 {
                //    continue;
                //}

                for ch in cell.record.rows.get(2).unwrap() {
                    tables.push(char::from_u32(*ch as u32).unwrap());
                }

                //tables.insert_str(
                //    tables.len(),
                //    &String::from_utf8_lossy(cell.record.rows.get(2).unwrap()),
                //);

                tables.push(' ');

                //print!(
                //    "{:?}",
                //    String::from_utf8_lossy(cell.record.rows.get(2).unwrap())
                //);

                //for row in cell.record.rows {
                //    let val = String::from_utf8_lossy(row.borrow());
                //    println!("{val}");
                //}
            }

            println!("{:?}", tables.trim());
        }

        _ => bail!("Missing or invalid command passed: {}", command),
    }

    Ok(())
}
