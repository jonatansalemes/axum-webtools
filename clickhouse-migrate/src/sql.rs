//! Lightweight ClickHouse SQL lexing: just enough to find comments, quoted
//! spans and statement boundaries without a full parser.

/// If a comment starts at byte `i`, returns the byte index just past it.
///
/// ClickHouse accepts `--` and `#` line comments and `/* ... */` block
/// comments, which nest. A line comment ends before its newline; an
/// unterminated block comment runs to the end of input.
fn comment_end(sql: &str, i: usize) -> Option<usize> {
    let bytes = sql.as_bytes();
    let len = bytes.len();
    match bytes[i] {
        b'-' if bytes.get(i + 1) == Some(&b'-') => {
            Some(sql[i..].find('\n').map(|n| i + n).unwrap_or(len))
        }
        b'#' => Some(sql[i..].find('\n').map(|n| i + n).unwrap_or(len)),
        b'/' if bytes.get(i + 1) == Some(&b'*') => {
            let mut depth = 1;
            let mut j = i + 2;
            while j < len && depth > 0 {
                if bytes[j] == b'/' && bytes.get(j + 1) == Some(&b'*') {
                    depth += 1;
                    j += 2;
                } else if bytes[j] == b'*' && bytes.get(j + 1) == Some(&b'/') {
                    depth -= 1;
                    j += 2;
                } else {
                    j += 1;
                }
            }
            Some(j)
        }
        _ => None,
    }
}

/// If a quoted span starts at byte `i`, returns the byte index just past it.
///
/// Covers `'string'` literals, `"identifier"` and `` `identifier` `` quoting
/// (all of which accept backslash escapes and a doubled quote), and
/// `$tag$ ... $tag$` heredocs. An unterminated span runs to the end of input.
fn quoted_end(sql: &str, i: usize) -> Option<usize> {
    let bytes = sql.as_bytes();
    let len = bytes.len();
    match bytes[i] {
        quote @ (b'\'' | b'"' | b'`') => {
            let mut j = i + 1;
            while j < len {
                if bytes[j] == b'\\' {
                    j += 2;
                } else if bytes[j] == quote {
                    if bytes.get(j + 1) == Some(&quote) {
                        j += 2; // doubled quote escapes itself
                    } else {
                        return Some(j + 1);
                    }
                } else {
                    j += 1;
                }
            }
            Some(len)
        }
        b'$' => {
            let tag_end = sql[i + 1..]
                .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
                .map(|n| i + 1 + n)
                .filter(|&t| bytes[t] == b'$')?;
            let delim = &sql[i..=tag_end];
            let body_start = tag_end + 1;
            Some(
                sql[body_start..]
                    .find(delim)
                    .map(|n| body_start + n + delim.len())
                    .unwrap_or(len),
            )
        }
        _ => None,
    }
}

/// Removes SQL comments from `sql` so a table name that only appears in a
/// comment does not trip safe mode. Each comment is replaced with a single
/// space to keep identifier boundaries intact.
///
/// Comment markers inside quoted strings, quoted identifiers or heredocs are
/// part of the literal, not comments, so those spans are copied verbatim.
pub fn strip_sql_comments(sql: &str) -> String {
    let len = sql.len();
    let mut out = String::with_capacity(len);
    let mut i = 0;

    while i < len {
        if let Some(end) = comment_end(sql, i) {
            out.push(' ');
            i = end;
        } else if let Some(end) = quoted_end(sql, i) {
            out.push_str(&sql[i..end.min(len)]);
            i = end;
        } else {
            // Advance by a whole char so multi-byte UTF-8 stays intact.
            let ch = sql[i..]
                .chars()
                .next()
                .expect("index is on a char boundary");
            out.push(ch);
            i += ch.len_utf8();
        }
    }

    out
}

/// Splits `sql` into individual statements on `;` boundaries.
///
/// The ClickHouse HTTP interface executes exactly one statement per request,
/// so migration files are split before being sent. Semicolons inside
/// comments, quoted spans and heredocs do not split. Each statement is
/// returned trimmed and without its terminating `;`; chunks containing only
/// whitespace and comments are dropped.
pub fn split_statements(sql: &str) -> Vec<String> {
    let len = sql.len();
    let mut statements = Vec::new();
    let mut start = 0;
    let mut i = 0;

    let mut push = |chunk: &str| {
        if !strip_sql_comments(chunk).trim().is_empty() {
            statements.push(chunk.trim().to_string());
        }
    };

    while i < len {
        if let Some(end) = comment_end(sql, i).or_else(|| quoted_end(sql, i)) {
            i = end;
        } else if sql.as_bytes()[i] == b';' {
            push(&sql[start..i]);
            i += 1;
            start = i;
        } else {
            i += sql[i..].chars().next().map(char::len_utf8).unwrap_or(1);
        }
    }
    push(&sql[start..len]);

    statements
}

/// Checks whether `content` references `table` as a whole SQL identifier.
///
/// Uses identifier boundaries (`[a-z0-9_]`) so a shorter name does not match
/// inside a longer one — e.g. `events` must not match `events_daily`.
/// Both `content` and `table` are expected to be lowercase.
pub fn content_references_table(content: &str, table: &str) -> bool {
    if table.is_empty() {
        return false;
    }

    let is_ident_char = |c: char| c.is_ascii_alphanumeric() || c == '_';
    let bytes = content.as_bytes();
    let table_len = table.len();
    let mut search_start = 0;

    while let Some(rel) = content[search_start..].find(table) {
        let start = search_start + rel;
        let end = start + table_len;

        let prev_is_ident = start
            .checked_sub(1)
            .map(|i| is_ident_char(bytes[i] as char))
            .unwrap_or(false);
        let next_is_ident = bytes
            .get(end)
            .map(|&b| is_ident_char(b as char))
            .unwrap_or(false);

        if !prev_is_ident && !next_is_ident {
            return true;
        }

        search_start = start + 1;
    }

    false
}

#[cfg(test)]
mod tests {
    use super::*;

    fn references_after_strip(sql: &str, table: &str) -> bool {
        content_references_table(&strip_sql_comments(sql).to_lowercase(), table)
    }

    #[test]
    fn test_content_references_table_exact_match() {
        let sql = "alter table events add column foo UInt8;".to_lowercase();
        assert!(content_references_table(&sql, "events"));
    }

    #[test]
    fn test_content_references_table_ignores_longer_name() {
        let sql = "alter table events_daily add column foo UInt8;".to_lowercase();
        assert!(!content_references_table(&sql, "events"));
        assert!(content_references_table(&sql, "events_daily"));
    }

    #[test]
    fn test_content_references_table_ignores_prefix_of_identifier() {
        let sql = "create table raw_events (id UInt64) engine = MergeTree order by id;";
        assert!(!content_references_table(&sql.to_lowercase(), "events"));
    }

    #[test]
    fn test_content_references_table_database_qualified() {
        let sql = "alter table analytics.events add column foo UInt8;".to_lowercase();
        assert!(content_references_table(&sql, "events"));
    }

    #[test]
    fn test_content_references_table_backtick_identifier() {
        let sql = "alter table `events` add column foo UInt8;".to_lowercase();
        assert!(content_references_table(&sql, "events"));
    }

    #[test]
    fn test_content_references_table_empty_table() {
        assert!(!content_references_table("alter table users;", ""));
    }

    #[test]
    fn test_strip_line_comments() {
        let sql = "alter table users add column foo UInt8; -- touches events later";
        assert!(!references_after_strip(sql, "events"));
        assert!(references_after_strip(sql, "users"));
    }

    #[test]
    fn test_strip_hash_comment() {
        let sql = "# cleanup for events\nalter table users add column foo UInt8;";
        assert!(!references_after_strip(sql, "events"));
    }

    #[test]
    fn test_strip_nested_block_comment() {
        let sql = "/* outer /* events */ still comment */ select 1;";
        assert!(!references_after_strip(sql, "events"));
    }

    #[test]
    fn test_comment_replaced_by_space_keeps_boundaries() {
        let sql = "alter table events/* comment */add column foo UInt8;";
        assert!(references_after_strip(sql, "events"));
    }

    #[test]
    fn test_comment_marker_inside_string_is_not_a_comment() {
        let sql = "insert into log (msg) values ('a -- b # c'); drop table events;";
        assert!(references_after_strip(sql, "events"));
    }

    #[test]
    fn test_backslash_escaped_quote_does_not_end_string() {
        let sql = "select 'it\\'s -- not a comment'; drop table events;";
        assert!(references_after_strip(sql, "events"));
    }

    #[test]
    fn test_heredoc_body_is_preserved() {
        let sql = "select $doc$ -- kept $doc$, 1 from events;";
        assert!(references_after_strip(sql, "events"));
    }

    #[test]
    fn test_strip_preserves_multibyte_text() {
        let sql = "select 'ação'; -- comentário";
        assert_eq!(strip_sql_comments(sql).trim_end(), "select 'ação';");
    }

    #[test]
    fn test_split_simple_statements() {
        let sql = "CREATE TABLE a (id UInt8) ENGINE = Memory;\nCREATE TABLE b (id UInt8) ENGINE = Memory;\n";
        assert_eq!(
            split_statements(sql),
            vec![
                "CREATE TABLE a (id UInt8) ENGINE = Memory",
                "CREATE TABLE b (id UInt8) ENGINE = Memory",
            ]
        );
    }

    #[test]
    fn test_split_without_trailing_semicolon() {
        assert_eq!(split_statements("SELECT 1"), vec!["SELECT 1"]);
    }

    #[test]
    fn test_split_ignores_semicolons_in_quotes_and_comments() {
        let sql = "INSERT INTO t VALUES ('a;b'); -- c;d\nSELECT \"x;y\", `p;q` /* r;s */ FROM t; SELECT $$u;v$$";
        assert_eq!(
            split_statements(sql),
            vec![
                "INSERT INTO t VALUES ('a;b')",
                "-- c;d\nSELECT \"x;y\", `p;q` /* r;s */ FROM t",
                "SELECT $$u;v$$",
            ]
        );
    }

    #[test]
    fn test_split_drops_comment_only_chunks() {
        let sql = "-- Add migration script here\nSELECT 1;\n-- trailing note\n;;";
        assert_eq!(
            split_statements(sql),
            vec!["-- Add migration script here\nSELECT 1"]
        );
    }

    #[test]
    fn test_split_empty_input() {
        assert!(split_statements("").is_empty());
        assert!(split_statements("-- only a comment\n").is_empty());
    }
}
