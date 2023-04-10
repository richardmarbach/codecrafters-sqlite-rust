use anyhow::{bail, Result};
use sqlite_starter_rust::{database::Database, record::Record, sql};

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
                    let row = database
                        .schema
                        .find_table(&command.table)
                        .ok_or(anyhow::anyhow!("Table not found: {}", command.table))?
                        .clone();

                    println!("schema: {}", row.sql);

                    let page = database.get_page(row.rootpage - 1)?;
                    let (_, definition) = sql::parse_creation(row.sql.as_bytes())
                        .map_err(|_e| anyhow::anyhow!("Failed to parse table definition"))?;
                    let field_count = definition.fields.len();

                    let records: Vec<Record> = page
                        .cells()
                        .map(|cell| match cell {
                            sqlite_starter_rust::page::Cell::LeafTable { payload, .. } => {
                                Ok(Record::read(payload, field_count))
                            }
                            _ => bail!("Unsupported cell type"),
                        })
                        .collect::<Result<Vec<_>>>()?;

                    let field_number = definition
                        .fields
                        .iter()
                        .position(|field| &field.name == command.fields.first().unwrap())
                        .expect("Field not found");

                    for record in records {
                        println!("{}", record.values[field_number]);
                    }
                }
                sql::SQLCommand::CreateTable(_) => bail!("Unsupported command: {}", query_string),
            };
        }
    }

    Ok(())
}
