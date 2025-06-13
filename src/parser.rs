use std::collections::HashMap;
use std::str::FromStr;

use once_cell::sync::Lazy;
use regex::{Regex, RegexSet};

pub fn parse_sql(sql: &str, replacement_map: &HashMap<&str, &str>) -> Option<QueryDetails> {
    let mut sql_sanitized = String::from_str(sql).unwrap();

    replacement_map
        .iter()
        .for_each(|(&from, &to)| sql_sanitized = sql_sanitized.replace(from, to));

    parse_sql_sanitized(&sql_sanitized)
}

fn parse_sql_sanitized(sql: &str) -> Option<QueryDetails> {
    let matches: Vec<_> = QUERY_SET.matches(sql).into_iter().collect();
    // println!("matches: {:?} {sql}", matches);
    match matches.as_slice() {
        [0] => {
            let regex = &REGEXES[0];
            let caps = regex.captures(sql).unwrap();
            let index_name = caps["index_name"].to_string();
            let table_name = caps["table_name"].to_string();
            let column_name = caps["column_name"].to_string();
            Some(QueryDetails {
                qtype: QueryType::INDEX(index_name),
                stmt: Statement {
                    table_name,
                    columns: vec![column_name],
                    filter: None,
                    is_star: None,
                },
            })
        }
        [1] => {
            let regex = &REGEXES[1];
            let caps = regex.captures(sql).unwrap();
            let cols: Vec<_> = caps["column_names"]
                .split(",")
                .map(|col_expr| {
                    col_expr
                        .trim()
                        .split(" ")
                        .collect::<Vec<_>>()
                        .first()
                        .unwrap()
                        .to_string()
                })
                .collect();

            Some(QueryDetails {
                qtype: QueryType::CREATE,
                stmt: Statement {
                    table_name: caps["table_name"].to_string(),
                    columns: cols,
                    filter: None,
                    is_star: None,
                },
            })
        }
        [2] => {
            let regex = &REGEXES[2];
            let caps = regex.captures(sql).unwrap();
            let count = caps.name("count").is_some();
            let filter = caps.name("filters").map(|_expr| {
                (
                    caps["filter_column"].to_string(),
                    caps["filter_value"].to_string(),
                )
            });
            let table_name = caps["table_name"].to_string();

            match caps.name("star") {
                Some(val) => {
                    let columns = vec![val.as_str().to_string()];
                    Some(QueryDetails {
                        qtype: QueryType::SELECT(count),
                        stmt: Statement {
                            table_name,
                            columns,
                            filter,
                            is_star: Some(true),
                        },
                    })
                }
                None => {
                    let columns = caps["column_names"]
                        .split(",")
                        .map(|name| String::from_str(name.trim()).unwrap())
                        .collect::<Vec<_>>();
                    Some(QueryDetails {
                        qtype: QueryType::SELECT(count),
                        stmt: Statement {
                            table_name,
                            columns,
                            filter,
                            is_star: Some(false),
                        },
                    })
                }
            }
        }
        [] => {
            println!("no matches");
            None
        }
        _ => {
            print!("multiple matches");
            None
        }
    }
}

static REGEXES: Lazy<Vec<Regex>> = Lazy::new(|| {
    let regexes = &[
        r"((CREATE|create) (INDEX|index) (?P<index_name>[A-Za-z_]+)on (?P<table_name>[A-Za-z_]+)? \((?P<column_name>.*)\))",
        r"((CREATE|create) (TABLE|table) (?P<table_name>[A-Za-z_]+)[\s]?\((?P<column_names>.*)\))",
        r"((SELECT|select) ((?<count>(COUNT|count)\()?((?<column_names>[ A-Za-z_,]+)|(?<star>\*)))\)? (FROM|from) (?P<table_name>[A-Za-z_]+))( (WHERE|where) (?<filters>(?<filter_column>[A-Za-z_]+)(\s)?=(\s)?'(?<filter_value>[\w\-() ]+)'))?",
    ];

    regexes
        .iter()
        .map(|regex_str| Regex::new(regex_str).unwrap())
        .collect()
});

static QUERY_SET: Lazy<RegexSet> = Lazy::new(|| {
    let regexes: Vec<&str> = REGEXES.iter().map(|rd| rd.as_str()).collect();
    RegexSet::new(&regexes).unwrap()
});

#[derive(Debug)]
pub struct QueryDetails {
    pub qtype: QueryType,
    pub stmt: Statement,
}

#[derive(Debug)]
pub struct Statement {
    pub table_name: String,
    pub columns: Vec<String>,
    pub filter: Option<(String, String)>,
    pub is_star: Option<bool>,
}

#[derive(Debug)]
pub enum QueryType {
    CREATE,
    SELECT(bool),
    INDEX(String),
}
