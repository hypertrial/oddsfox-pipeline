use std::path::Path;

use crate::error::Result;

pub fn run_adhoc(out: &Path, db: &Path, query: &str, limit: usize) -> Result<()> {
    run_adhoc_writer(out, db, query, limit, &mut std::io::stdout())
}

pub fn run_adhoc_writer(
    out: &Path,
    db: &Path,
    query: &str,
    limit: usize,
    writer: &mut impl std::io::Write,
) -> Result<()> {
    let conn = if db.exists() {
        crate::duckdb_engine::open_connection(Some(db))?
    } else {
        let options = crate::config::DuckDbOptions {
            out: out.to_path_buf(),
            db: db.to_path_buf(),
        };
        crate::duckdb::run(&options)?;
        crate::duckdb_engine::open_connection(Some(db))?
    };
    let mut stmt = conn.prepare(query)?;
    let mut rows = stmt.query([])?;
    let column_names = rows
        .as_ref()
        .expect("query should expose statement metadata")
        .column_names();
    writeln!(writer, "{}", column_names.join("\t"))?;

    let mut count = 0;
    while let Some(row) = rows.next()? {
        if limit != 0 && count >= limit {
            let row_word = if limit == 1 { "row" } else { "rows" };
            writeln!(
                writer,
                "(truncated after {limit} {row_word}; rerun with --limit 0 for all rows)"
            )?;
            break;
        }
        let mut values = Vec::with_capacity(column_names.len());
        for idx in 0..column_names.len() {
            values.push(crate::duckdb_engine::format_value(row.get_ref(idx)?));
        }
        writeln!(writer, "{}", values.join("\t"))?;
        count += 1;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sql_output_prints_headers_columns_and_nulls() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("test.duckdb");
        duckdb::Connection::open(&db)
            .unwrap()
            .execute(
                "CREATE VIEW sample AS SELECT 1 AS id, NULL AS missing, 'yes' AS label",
                [],
            )
            .unwrap();

        let mut out = Vec::new();
        run_adhoc_writer(
            dir.path(),
            &db,
            "SELECT id, missing, label FROM sample",
            100,
            &mut out,
        )
        .unwrap();

        assert_eq!(
            String::from_utf8(out).unwrap(),
            "id\tmissing\tlabel\n1\t\tyes\n"
        );
    }

    #[test]
    fn sql_output_limit_one_truncates() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("test.duckdb");
        duckdb::Connection::open(&db)
            .unwrap()
            .execute(
                "CREATE VIEW sample AS SELECT 1 AS id UNION ALL SELECT 2",
                [],
            )
            .unwrap();

        let mut out = Vec::new();
        run_adhoc_writer(
            dir.path(),
            &db,
            "SELECT id FROM sample ORDER BY id",
            1,
            &mut out,
        )
        .unwrap();

        assert_eq!(
            String::from_utf8(out).unwrap(),
            "id\n1\n(truncated after 1 row; rerun with --limit 0 for all rows)\n"
        );
    }

    #[test]
    fn sql_output_limit_zero_prints_all_rows() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("test.duckdb");
        duckdb::Connection::open(&db)
            .unwrap()
            .execute(
                "CREATE VIEW sample AS SELECT 1 AS id UNION ALL SELECT 2",
                [],
            )
            .unwrap();

        let mut out = Vec::new();
        run_adhoc_writer(
            dir.path(),
            &db,
            "SELECT id FROM sample ORDER BY id",
            0,
            &mut out,
        )
        .unwrap();

        assert_eq!(String::from_utf8(out).unwrap(), "id\n1\n2\n");
    }
}
