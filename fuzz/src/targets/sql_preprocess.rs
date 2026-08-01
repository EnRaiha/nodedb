const MAX_SQL_BYTES: usize = 256 * 1024;

pub fn run(data: &[u8]) {
    let bounded = &data[..data.len().min(MAX_SQL_BYTES)];
    if let Ok(sql) = std::str::from_utf8(bounded) {
        let _ = nodedb_sql::parser::preprocess::preprocess(sql);
        let _ = nodedb_sql::parser::statement::parse_sql(sql);
        let _ = nodedb_sql::parse_expr_string(sql);
    }
}
