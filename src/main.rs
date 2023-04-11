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
                    let row = database
                        .schema
                        .find_table(&command.table)
                        .ok_or(anyhow::anyhow!("Table not found: {}", command.table))?
                        .clone();

                    let page = database.get_page(row.rootpage - 1)?;
                    let (_, definition) = sql::parse_creation(row.sql.as_bytes())
                        .map_err(|_e| anyhow::anyhow!("Failed to parse table definition"))?;

                    let fields = command
                        .fields
                        .iter()
                        .map(|sql_field| {
                            definition.find_field(sql_field).expect("Fields not found")
                        })
                        .collect::<Vec<_>>();

                    let records: Vec<Record> = page
                        .cells()
                        .map(|cell| match cell {
                            sqlite_starter_rust::page::Cell::LeafTable { payload, .. } => {
                                Ok(Record::read(payload))
                            }
                            _ => bail!("Unsupported cell type"),
                        })
                        .filter(|record| {
                            let Ok(record) = record else { return true; };
                            let Some(ref where_clause) = command.where_clause else {
                                return true;
                            };

                            let (pos, _field) = definition
                                .find_field(&where_clause.field)
                                .expect("Field not found");

                            format!("{}", record.values[pos]) == where_clause.value
                        })
                        .collect::<Result<Vec<_>>>()?;

                    let fields = fields.iter().map(|(pos, _)| pos).collect::<Vec<_>>();

                    for record in records {
                        let values = fields
                            .iter()
                            .map(|i| format!("{}", record.values[**i]))
                            .collect::<Vec<_>>()
                            .join("|");
                        println!("{}", values);
                    }
                }
                sql::SQLCommand::CreateTable(_) => bail!("Unsupported command: {}", query_string),
            };
        }
    }

    Ok(())
}
