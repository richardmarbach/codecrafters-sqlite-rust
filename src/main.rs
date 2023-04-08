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
            let table_name = query
                .split(" ")
                .last()
                .ok_or(anyhow::anyhow!("Invalid command passed: {}", command))?;

            let row = database
                .schema
                .find_table(table_name)
                .ok_or(anyhow::anyhow!("Table not found: {}", table_name))?;

            let page = database.get_page(row.rootpage - 1)?;
            println!("{}", page.header.number_of_cells);
        }
    }

    Ok(())
}
