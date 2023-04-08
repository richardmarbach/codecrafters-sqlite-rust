use nom::{
    branch::alt,
    bytes::complete::{tag, tag_no_case, take_while1},
    character::{
        complete::{alphanumeric1, multispace0, multispace1},
        is_alphanumeric,
    },
    combinator::{map, opt},
    multi::many1,
    sequence::{delimited, terminated, tuple},
    IResult,
};

#[derive(Debug, PartialEq)]
pub enum SelectStatement {
    Fields(SelectFields),
    Count(String),
}

#[derive(Debug, PartialEq)]
pub struct SelectFields {
    pub fields: Vec<String>,
    pub table: String,
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

#[derive(Debug, PartialEq)]
pub enum SQLCommand {
    Select(SelectStatement),
    CreateTable(CreateTableStatement),
}

pub fn parse(input: &[u8]) -> IResult<&[u8], SQLCommand> {
    alt((
        map(creation, |c| SQLCommand::CreateTable(c)),
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
    let (remaining_input, (_, _, fields, _, _, _, table, _)) = tuple((
        tag_no_case("select"),
        multispace1,
        fields,
        multispace0,
        tag_no_case("from"),
        multispace1,
        identifier,
        opt(tag(";")),
    ))(input)?;

    Ok((
        remaining_input,
        SelectStatement::Fields(SelectFields { table, fields }),
    ))
}

fn fields(input: &[u8]) -> IResult<&[u8], Vec<String>> {
    many1(terminated(
        identifier,
        opt(delimited(multispace0, tag(","), multispace0)),
    ))(input)
}

fn creation(input: &[u8]) -> IResult<&[u8], CreateTableStatement> {
    let (remaining_input, (_, _, _, _, table, _, _, _, fields, _, _, _)) = tuple((
        tag_no_case("create"),
        multispace1,
        tag_no_case("table"),
        multispace1,
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

fn field_specification(input: &[u8]) -> IResult<&[u8], Field> {
    let (remaining_input, (column, _, _, _)) = tuple((
        alphanumeric1,
        opt(delimited(multispace0, alphanumeric1, multispace0)),
        opt(delimited(
            multispace0,
            tag_no_case("PRIMARY KEY AUTOINCREMENT"),
            multispace0,
        )),
        opt(delimited(multispace0, tag(","), multispace0)),
    ))(input)?;
    let name = String::from_utf8(column.to_vec()).unwrap();
    Ok((remaining_input, Field { name }))
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
                fields: vec!["id".to_string()]
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
                fields: vec!["id".to_string(), "name".to_string()]
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
        let input = b"CREATE TABLE test (id INTEGER primary key autoincrement)";
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
        let input = b"CREATE TABLE test (id INTEGER, name TEXT)";
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
