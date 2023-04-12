use nom::{
    branch::alt,
    bytes::complete::{tag, tag_no_case, take_until, take_while1},
    character::{
        complete::{multispace0, multispace1},
        is_alphanumeric, is_space,
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
pub enum ColumnConstraint {
    PrimaryKey,
}

#[derive(Debug, PartialEq)]
pub struct Field {
    pub name: String,
    pub is_primary_key: bool,
}

impl Field {
    pub fn new(name: String) -> Self {
        Self {
            name,
            is_primary_key: false,
        }
    }
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
pub struct CreateIndexStatement {
    pub name: String,
    pub table: String,
    pub fields: Vec<String>,
}

#[derive(Debug, PartialEq)]
pub enum SQLCommand {
    Select(SelectStatement),
    CreateTable(CreateTableStatement),
    CreateIndex(CreateIndexStatement),
}

pub fn parse(input: &[u8]) -> IResult<&[u8], SQLCommand> {
    alt((
        map(parse_creation, |c| SQLCommand::CreateTable(c)),
        map(selection, |s| SQLCommand::Select(s)),
        map(count_selection, |s| SQLCommand::Select(s)),
        map(parse_index_creation, |c| SQLCommand::CreateIndex(c)),
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
        tag("'"),
        take_until("'"),
    )))(input)?;

    let maybe_where = if let Some((_, _, _, field, _, _, _, _, value)) = maybe_where {
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

pub fn parse_index_creation(input: &[u8]) -> IResult<&[u8], CreateIndexStatement> {
    let (remaining_input, (_, _, _, _, _, name, _, _, _, table, _, _, _, columns, _, _, _)) =
        tuple((
            tag_no_case("create"),
            multispace1,
            tag_no_case("index"),
            multispace1,
            opt(tuple((tag_no_case("IF NOT EXISTS"), multispace1))),
            identifier, // index name
            multispace1,
            tag_no_case("ON"),
            multispace1,
            identifier, // table
            multispace0,
            tag("("),
            multispace0,
            many1(identifier), // columns
            multispace0,
            tag(")"),
            opt(tag(";")),
        ))(input)?;

    Ok((
        remaining_input,
        CreateIndexStatement {
            name,
            table,
            fields: columns,
        },
    ))
}

fn identifier(input: &[u8]) -> IResult<&[u8], String> {
    let (input, name) = alt((
        delimited(
            tag("\""),
            take_while1(is_sql_identifier_with_space),
            tag("\""),
        ),
        take_while1(is_sql_identifier),
    ))(input)?;
    let name = String::from_utf8(name.to_vec()).unwrap();

    Ok((input, name))
}

fn is_sql_identifier_with_space(chr: u8) -> bool {
    is_alphanumeric(chr) || chr == b'_' || is_space(chr)
}

fn is_sql_identifier(chr: u8) -> bool {
    is_alphanumeric(chr) || chr == b'_'
}

fn field_specification_list(input: &[u8]) -> IResult<&[u8], Vec<Field>> {
    many1(field_specification)(input)
}

fn column_constraint(input: &[u8]) -> IResult<&[u8], Option<ColumnConstraint>> {
    let not_null = map(
        delimited(multispace0, tag_no_case("NOT NULL"), multispace0),
        |_| None,
    );
    let auto_increment = map(
        delimited(multispace0, tag_no_case("AUTOINCREMENT"), multispace0),
        |_| None,
    );
    let primary_key = map(
        delimited(multispace0, tag_no_case("PRIMARY KEY"), multispace0),
        |_| Some(ColumnConstraint::PrimaryKey),
    );

    alt((not_null, auto_increment, primary_key))(input)
}

fn field_specification(input: &[u8]) -> IResult<&[u8], Field> {
    let (remaining_input, (column, ty, constraints, _)) = tuple((
        identifier,
        opt(delimited(multispace0, identifier, multispace0)), // type
        many0(column_constraint),
        opt(delimited(multispace0, tag(","), multispace0)),
    ))(input)?;

    let is_primary_key = constraints
        .iter()
        .flatten()
        .find(|c| **c == ColumnConstraint::PrimaryKey)
        .is_some()
        && ty
            .map(|ty| ty.to_ascii_lowercase() == "integer")
            .unwrap_or(false);

    Ok((
        remaining_input,
        Field {
            name: column,
            is_primary_key,
        },
    ))
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
        let input = b"SELECT id, name FROM test WHERE super_name = 'test string'";
        let (_, result) = parse(input).unwrap();

        assert_eq!(
            result,
            SQLCommand::Select(SelectStatement::Fields(SelectFields {
                table: "test".to_string(),
                fields: vec!["id".to_string(), "name".to_string()],
                where_clause: Some(WhereClause {
                    field: "super_name".to_string(),
                    value: "test string".to_string()
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
                    name: "id".to_string(),
                    is_primary_key: true
                },]
            })
        );
    }

    #[test]
    fn parse_create_table_with_two_entries() {
        let input = b"CREATE TABLE \"test\" (id INTEGER primary key, \"name field\" TEXT NOT NULL)";
        let (_, result) = parse(input).unwrap();

        assert_eq!(
            result,
            SQLCommand::CreateTable(CreateTableStatement {
                table: "test".to_string(),
                fields: vec![
                    Field {
                        name: "id".to_string(),
                        is_primary_key: true
                    },
                    Field::new("name field".to_string())
                ]
            })
        );
    }

    #[test]
    fn parse_create_super_heroes() {
        let input = b"CREATE TABLE IF NOT EXISTS \"superheroes\" (id integer primary key autoincrement, name text not null, eye_color text, hair_color text, appearance_count integer, first_appearance text, first_appearance_year text);";
        let (_, result) = parse(input).unwrap();

        assert_eq!(
            result,
            SQLCommand::CreateTable(CreateTableStatement {
                table: "superheroes".to_string(),
                fields: vec![
                    Field {
                        name: "id".to_string(),
                        is_primary_key: true,
                    },
                    Field::new("name".to_string()),
                    Field::new("eye_color".to_string()),
                    Field::new("hair_color".to_string()),
                    Field::new("appearance_count".to_string()),
                    Field::new("first_appearance".to_string()),
                    Field::new("first_appearance_year".to_string())
                ]
            })
        );
    }
    #[test]
    fn parse_create_index() {
        let input = b"CREATE INDEX idx_companies_country on companies (country);";
        let (_, result) = parse(input).unwrap();

        assert_eq!(
            result,
            SQLCommand::CreateIndex(CreateIndexStatement {
                table: "companies".to_string(),
                name: "idx_companies_country".to_string(),
                fields: vec!["country".to_string()],
            })
        );
    }
}
