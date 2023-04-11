use std::io::stdout;

use anyhow::{bail, Result};
use sqlite_starter_rust::{database::Database, sql};

fn main() -> Result<()> {
    // Parse arguments
    let args = std::env::args().collect::<Vec<_>>();
    match args.len() {
        0 | 1 => bail!("Missing <database path> and <command>"),
        2 => bail!("Missing <command>"),
        _ => {}
    }

    {}

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

        query_string => {
            let (_, query) = sql::parse(query_string.as_bytes())
                .map_err(|_e| anyhow::anyhow!("Failed to parse query"))?;

            match query {
                sql::SQLCommand::Select(sql::SelectStatement::Count(table)) => {
                    let row = database
                        .schema
                        .find_table(&table)
                        .ok_or(anyhow::anyhow!("Table not found: {}", table))?;
                    let page = database.get_page(row.rootpage - 1)?;
                    println!("{}", page.header.number_of_cells);
                }
                sql::SQLCommand::Select(sql::SelectStatement::Fields(command)) => {
                    database.select_fields(&command, &mut stdout())?;
                }
                sql::SQLCommand::CreateTable(_) => bail!("Unsupported command: {}", query_string),
            };
        }
    }

    Ok(())
}
