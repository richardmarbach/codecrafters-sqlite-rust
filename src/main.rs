use anyhow::{bail, Result};
use sqlite_starter_rust::database::Database;

fn main() -> Result<()> {
    // Parse arguments
    let args = std::env::args().collect::<Vec<_>>();
    match args.len() {
        0 | 1 => bail!("Missing <database path> and <command>"),
        2 => bail!("Missing <command>"),
        _ => {}
    }

    let mut database = Database::open(&args[1])?;

    // Parse command and act accordingly
    let command = &args[2];
    match command.as_str() {
        ".dbinfo" => {
            println!("database page size: {}", database.header.page_size);
            println!(
                "number of tables: {}",
                database.schema.user_tables().count()
            );
        }
        ".tables" => database
            .schema
            .user_tables()
            .for_each(|row| println!("{}", row.name)),
        query => {
            if !query.starts_with("SELECT COUNT(*) FROM ") {
                bail!("Invalid command passed: {}", command);
            }

            let table_name = query.split(" ").last().map_or_else(
                || bail!("Invalid command passed: {}", command),
                |s| Ok(s.trim()),
            )?;

            let row = database
                .schema
                .find_table(table_name)
                .map_or_else(|| bail!("Table not found: {}", table_name), Ok)?;

            let page = database.get_page(row.rootpage - 1)?;
            println!("{}", page.header.number_of_cells);
        }
    }

    Ok(())
}
