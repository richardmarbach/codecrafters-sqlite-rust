use nom::{
    branch::alt,
    bytes::complete::{tag, tag_no_case, take_while1},
    character::{
        complete::{alphanumeric0, alphanumeric1, multispace0, multispace1},
        is_alphanumeric,
    },
    combinator::{map, opt},
    multi::{many0, many1},
    sequence::{delimited, terminated, tuple},
    IResult,
};

#[derive(Debug, PartialEq)]
pub enum SelectStatement {
    Fields(SelectFields),
    Count(String),
}

#[derive(Debug, PartialEq)]
pub struct WhereClause {
    pub field: String,
    pub value: String,
}

#[derive(Debug, PartialEq)]
pub struct SelectFields {
    pub fields: Vec<String>,
    pub table: String,
    pub where_clause: Option<WhereClause>,
}

#[derive(Debug, PartialEq)]
pub struct Field {
    pub name: String,
}

#[derive(Debug, PartialEq)]
pub struct CreateTableStatement {
    pub table: String,
    pub fields: Vec<Field>,
}

impl CreateTableStatement {
    pub fn find_field(&self, field_name: &str) -> Option<(usize, &Field)> {
        self.fields
            .iter()
            .enumerate()
            .find(|(_, field)| field.name == field_name)
    }
}

#[derive(Debug, PartialEq)]
pub enum SQLCommand {
    Select(SelectStatement),
    CreateTable(CreateTableStatement),
}

pub fn parse(input: &[u8]) -> IResult<&[u8], SQLCommand> {
    alt((
        map(parse_creation, |c| SQLCommand::CreateTable(c)),
        map(selection, |s| SQLCommand::Select(s)),
        map(count_selection, |s| SQLCommand::Select(s)),
    ))(input)
}

fn count_selection(input: &[u8]) -> IResult<&[u8], SelectStatement> {
    let (remaining_input, (_, _, _, _, _, _, table, _)) = tuple((
        tag_no_case("select"),
        multispace1,
        tag_no_case("count(*)"),
        multispace1,
        tag_no_case("from"),
        multispace1,
        identifier,
        opt(tag(";")),
    ))(input)?;

    Ok((remaining_input, SelectStatement::Count(table)))
}

fn selection(input: &[u8]) -> IResult<&[u8], SelectStatement> {
    let (remaining_input, (_, _, fields, _, _, _, table, where_clause, _)) = tuple((
        tag_no_case("select"),
        multispace1,
        identifiers,
        multispace0,
        tag_no_case("from"),
        multispace1,
        identifier,
        parse_where_clause,
        opt(tag(";")),
    ))(input)?;

    Ok((
        remaining_input,
        SelectStatement::Fields(SelectFields {
            table,
            fields,
            where_clause,
        }),
    ))
}

fn identifiers(input: &[u8]) -> IResult<&[u8], Vec<String>> {
    many1(terminated(
        identifier,
        opt(delimited(multispace0, tag(","), multispace0)),
    ))(input)
}

fn parse_where_clause(input: &[u8]) -> IResult<&[u8], Option<WhereClause>> {
    let (remaining_input, maybe_where) = opt(tuple((
        multispace0,
        tag_no_case("where"),
        multispace0,
        identifier,
        multispace0,
        tag("="),
        multispace0,
        delimited(tag("'"), alphanumeric0, tag("'")),
    )))(input)?;

    let maybe_where = if let Some((_, _, _, field, _, _, _, value)) = maybe_where {
        let value = String::from_utf8(value.to_vec()).unwrap();
        Some(WhereClause { field, value })
    } else {
        None
    };

    Ok((remaining_input, maybe_where))
}

pub fn parse_creation(input: &[u8]) -> IResult<&[u8], CreateTableStatement> {
    let (remaining_input, (_, _, _, _, _, table, _, _, _, fields, _, _, _)) = tuple((
        tag_no_case("create"),
        multispace1,
        tag_no_case("table"),
        multispace1,
        opt(tuple((tag_no_case("IF NOT EXISTS"), multispace1))),
        identifier,
        multispace0,
        tag("("),
        multispace0,
        field_specification_list,
        multispace0,
        tag(")"),
        opt(tag(";")),
    ))(input)?;

    Ok((remaining_input, CreateTableStatement { table, fields }))
}

fn identifier(input: &[u8]) -> IResult<&[u8], String> {
    let (input, name) = take_while1(is_alphanumeric)(input)?;
    let name = String::from_utf8(name.to_vec()).unwrap();

    Ok((input, name))
}

fn field_specification_list(input: &[u8]) -> IResult<&[u8], Vec<Field>> {
    many1(field_specification)(input)
}

fn column_constraint(input: &[u8]) -> IResult<&[u8], &[u8]> {
    let not_null = delimited(multispace0, tag_no_case("NOT NULL"), multispace0);
    let auto_increment = delimited(multispace0, tag_no_case("AUTOINCREMENT"), multispace0);
    let primary_key = delimited(multispace0, tag_no_case("PRIMARY KEY"), multispace0);

    alt((not_null, auto_increment, primary_key))(input)
}

fn field_specification(input: &[u8]) -> IResult<&[u8], Field> {
    let (remaining_input, (column, _, _, _)) = tuple((
        identifier,
        opt(delimited(multispace0, alphanumeric1, multispace0)), // type
        many0(column_constraint),
        opt(delimited(multispace0, tag(","), multispace0)),
    ))(input)?;
    Ok((remaining_input, Field { name: column }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_select_with_one_field() {
        let input = b"SELECT id FROM test";
        let (_, result) = parse(input).unwrap();

        assert_eq!(
            result,
            SQLCommand::Select(SelectStatement::Fields(SelectFields {
                table: "test".to_string(),
                fields: vec!["id".to_string()],
                where_clause: None
            }))
        );
    }

    #[test]
    fn parse_select_with_two_fields() {
        let input = b"SELECT id, name FROM test";
        let (_, result) = parse(input).unwrap();

        assert_eq!(
            result,
            SQLCommand::Select(SelectStatement::Fields(SelectFields {
                table: "test".to_string(),
                fields: vec!["id".to_string(), "name".to_string()],
                where_clause: None
            }))
        );
    }

    #[test]
    fn parse_select_with_where() {
        let input = b"SELECT id, name FROM test WHERE name = 'test'";
        let (_, result) = parse(input).unwrap();

        assert_eq!(
            result,
            SQLCommand::Select(SelectStatement::Fields(SelectFields {
                table: "test".to_string(),
                fields: vec!["id".to_string(), "name".to_string()],
                where_clause: Some(WhereClause {
                    field: "name".to_string(),
                    value: "test".to_string()
                })
            }))
        );
    }

    #[test]
    fn parse_select_with_count() {
        let input = b"SELECT COUNT(*) FROM test";
        let (_, result) = parse(input).unwrap();

        assert_eq!(
            result,
            SQLCommand::Select(SelectStatement::Count("test".to_string()))
        );
    }

    #[test]
    fn parse_create_table_with_one_entry() {
        let input = b"CREATE TABLE IF NOT EXISTS test (id INTEGER primary key autoincrement)";
        let (_, result) = parse(input).unwrap();

        assert_eq!(
            result,
            SQLCommand::CreateTable(CreateTableStatement {
                table: "test".to_string(),
                fields: vec![Field {
                    name: "id".to_string()
                },]
            })
        );
    }

    #[test]
    fn parse_create_table_with_two_entries() {
        let input = b"CREATE TABLE test (id INTEGER primary key, name TEXT NOT NULL)";
        let (_, result) = parse(input).unwrap();

        assert_eq!(
            result,
            SQLCommand::CreateTable(CreateTableStatement {
                table: "test".to_string(),
                fields: vec![
                    Field {
                        name: "id".to_string()
                    },
                    Field {
                        name: "name".to_string()
                    }
                ]
            })
        );
    }
}
