use crate::schema::{CollectionSchema, TableSchema};

mod cleanup;
mod ddl;
mod drop_all_table;
mod fetch_objects;
mod upsert;

pub fn cleanup(table: &str, schema: &TableSchema) -> String {
    let mut out = String::new();
    cleanup::generate(&mut out, table, schema).unwrap();
    out
}

pub fn ddl(schema: &CollectionSchema) -> String {
    let mut out = String::new();
    ddl::generate(&mut out, schema).unwrap();
    out
}

pub fn fetch_objects(schema: &CollectionSchema) -> String {
    let mut out = String::new();
    fetch_objects::generate(&mut out, schema).unwrap();
    out
}

pub fn upsert(table: &str, schema: &TableSchema) -> String {
    let mut out = String::new();
    upsert::generate(&mut out, table, schema).unwrap();
    out
}

pub fn drop_all_tables(schema: &CollectionSchema) -> String {
    let mut out = String::new();
    drop_all_table::generate(&mut out, schema).unwrap();
    out
}
