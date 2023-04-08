use nom::{
    bytes::complete::{tag, tag_no_case, take_while1},
    character::{
        complete::{alphanumeric1, multispace0, multispace1},
        is_alphanumeric,
    },
    combinator::{map, opt},
    multi::many1,
    sequence::{delimited, tuple},
    IResult,
};

#[derive(Debug, PartialEq)]
pub struct SelectStatement {
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
    map(creation, |c| SQLCommand::CreateTable(c))(input)
}

fn creation(input: &[u8]) -> IResult<&[u8], CreateTableStatement> {
    let (remaining_input, (_, _, _, _, table, _, _, _, fields, _, _)) = tuple((
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
    let (remaining_input, (column, _, _)) = tuple((
        alphanumeric1,
        opt(delimited(multispace1, alphanumeric1, multispace0)),
        opt(delimited(multispace0, tag(","), multispace0)),
    ))(input)?;
    let name = String::from_utf8(column.to_vec()).unwrap();
    Ok((remaining_input, Field { name }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_create_table() {
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
