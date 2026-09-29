use urlencoding::decode;

const DEFAULT_HTTP_PORT: &str = "8123";
const DEFAULT_HTTPS_PORT: &str = "8443";
const DEFAULT_USER: &str = "default";
const DEFAULT_DATABASE: &str = "default";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClickHouseConnectionInfo {
    /// `http` or `https`.
    pub scheme: String,
    pub host: String,
    pub port: String,
    pub user: String,
    pub password: Option<String>,
    pub database: String,
    /// Extra query-string parameters, forwarded to ClickHouse as settings on
    /// every request (e.g. `?max_execution_time=600`).
    pub settings: Vec<(String, String)>,
}

fn decode_component(value: &str, what: &str) -> Result<String, Box<dyn std::error::Error>> {
    Ok(decode(value)
        .map_err(|e| format!("Invalid UTF-8 in {} after URL decoding: {}", what, e))?
        .into_owned())
}

/// Parses a ClickHouse connection URL into its components.
///
/// Accepted schemes are `http://`, `https://` and `clickhouse://` (an alias for
/// `http://`). The shape is `scheme://user:password@host:port/database?setting=value`,
/// where everything except the host is optional.
///
/// # Arguments
/// * `url` - ClickHouse HTTP interface URL
///
/// # Returns
/// * A ClickHouseConnectionInfo struct with parsed connection details
pub fn parse_clickhouse_url(
    url: &str,
) -> Result<ClickHouseConnectionInfo, Box<dyn std::error::Error>> {
    let (scheme, rest) = if let Some(rest) = url.strip_prefix("https://") {
        ("https", rest)
    } else if let Some(rest) = url.strip_prefix("http://") {
        ("http", rest)
    } else if let Some(rest) = url.strip_prefix("clickhouse://") {
        ("http", rest)
    } else {
        return Err(
            "Invalid ClickHouse URL: must start with http://, https:// or clickhouse://".into(),
        );
    };

    let (before_query, query) = rest.split_once('?').unwrap_or((rest, ""));
    let (auth_host, db_name) = before_query.split_once('/').unwrap_or((before_query, ""));

    let (auth, host_port) = match auth_host.rsplit_once('@') {
        Some((a, h)) => (Some(a), h),
        None => (None, auth_host),
    };

    let (user, password) = match auth {
        Some(auth_str) => {
            let (u, p) = auth_str.split_once(':').unwrap_or((auth_str, ""));
            (
                Some(decode_component(u, "username")?),
                if p.is_empty() {
                    None
                } else {
                    Some(decode_component(p, "password")?)
                },
            )
        }
        None => (None, None),
    };

    let (host, port) = match host_port.rsplit_once(':') {
        Some((h, p)) => (h.to_string(), Some(p.to_string())),
        None => (host_port.to_string(), None),
    };

    let mut settings = Vec::new();
    for pair in query.split('&').filter(|p| !p.is_empty()) {
        let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
        settings.push((
            decode_component(k, "query parameter")?,
            decode_component(v, "query parameter")?,
        ));
    }

    let db_name = db_name.trim_end_matches('/');

    Ok(ClickHouseConnectionInfo {
        scheme: scheme.to_string(),
        host: if host.is_empty() {
            "localhost".to_string()
        } else {
            host
        },
        port: port.unwrap_or_else(|| {
            if scheme == "https" {
                DEFAULT_HTTPS_PORT.to_string()
            } else {
                DEFAULT_HTTP_PORT.to_string()
            }
        }),
        user: user
            .filter(|u| !u.is_empty())
            .unwrap_or_else(|| DEFAULT_USER.to_string()),
        password,
        database: if db_name.is_empty() {
            DEFAULT_DATABASE.to_string()
        } else {
            decode_component(db_name, "database name")?
        },
        settings,
    })
}

/// Minimal client for the ClickHouse HTTP interface.
///
/// Every call sends exactly one statement as the request body, which is the
/// only form the HTTP interface accepts. SQL is sent verbatim — no placeholder
/// substitution — so migration files reach the server exactly as written.
pub struct ClickHouseClient {
    http: reqwest::Client,
    endpoint: String,
    info: ClickHouseConnectionInfo,
}

impl ClickHouseClient {
    /// Builds a client from a ClickHouse connection URL.
    pub fn connect(url: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let info = parse_clickhouse_url(url)?;
        let endpoint = format!("{}://{}:{}/", info.scheme, info.host, info.port);
        let http = reqwest::Client::builder().build()?;
        Ok(Self {
            http,
            endpoint,
            info,
        })
    }

    async fn send(&self, sql: &str) -> Result<String, Box<dyn std::error::Error>> {
        let mut params: Vec<(&str, &str)> = vec![
            ("database", self.info.database.as_str()),
            // Buffer the response server-side so an error raised mid-query is
            // reported as an HTTP error instead of a 200 with a truncated body.
            ("wait_end_of_query", "1"),
        ];
        params.extend(
            self.info
                .settings
                .iter()
                .map(|(k, v)| (k.as_str(), v.as_str())),
        );

        let mut request = self
            .http
            .post(&self.endpoint)
            .query(&params)
            .header("X-ClickHouse-User", &self.info.user)
            .body(sql.to_string());
        if let Some(password) = &self.info.password {
            request = request.header("X-ClickHouse-Key", password);
        }

        let response = request.send().await?;
        let status = response.status();
        let body = response.text().await?;

        if !status.is_success() {
            return Err(format!("ClickHouse returned {}: {}", status, body.trim()).into());
        }
        Ok(body)
    }

    /// Executes a single statement, discarding any result.
    pub async fn execute(&self, sql: &str) -> Result<(), Box<dyn std::error::Error>> {
        self.send(sql).await.map(|_| ())
    }

    /// Runs a query and returns its rows as tab-separated fields.
    ///
    /// The query must not carry its own `FORMAT` clause. Values are returned
    /// unescaped as ClickHouse wrote them, which is fine for the numeric and
    /// hex columns this tool reads.
    pub async fn query_rows(
        &self,
        sql: &str,
    ) -> Result<Vec<Vec<String>>, Box<dyn std::error::Error>> {
        let body = self
            .send(&format!("{} FORMAT TabSeparated", sql.trim_end()))
            .await?;
        Ok(body
            .lines()
            .filter(|l| !l.is_empty())
            .map(|l| l.split('\t').map(str::to_string).collect())
            .collect())
    }
}

/// Quotes a value as a ClickHouse string literal.
pub fn quote_string(value: &str) -> String {
    format!("'{}'", value.replace('\\', "\\\\").replace('\'', "\\'"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_full_url() {
        let info = parse_clickhouse_url("http://user:pass@localhost:8123/mydb").unwrap();
        assert_eq!(info.scheme, "http");
        assert_eq!(info.user, "user");
        assert_eq!(info.password, Some("pass".to_string()));
        assert_eq!(info.host, "localhost");
        assert_eq!(info.port, "8123");
        assert_eq!(info.database, "mydb");
        assert!(info.settings.is_empty());
    }

    #[test]
    fn applies_defaults() {
        let info = parse_clickhouse_url("http://localhost").unwrap();
        assert_eq!(info.user, "default");
        assert_eq!(info.password, None);
        assert_eq!(info.port, "8123");
        assert_eq!(info.database, "default");
    }

    #[test]
    fn https_defaults_to_secure_port() {
        let info = parse_clickhouse_url("https://ch.example.com/analytics").unwrap();
        assert_eq!(info.scheme, "https");
        assert_eq!(info.port, "8443");
        assert_eq!(info.database, "analytics");
    }

    #[test]
    fn clickhouse_scheme_is_http_alias() {
        let info = parse_clickhouse_url("clickhouse://u:p@db:9999/x").unwrap();
        assert_eq!(info.scheme, "http");
        assert_eq!(info.port, "9999");
    }

    #[test]
    fn decodes_encoded_credentials_and_database() {
        let info =
            parse_clickhouse_url("http://us%40er:p%40ss%3Aword@localhost:8123/my%2Ddb").unwrap();
        assert_eq!(info.user, "us@er");
        assert_eq!(info.password, Some("p@ss:word".to_string()));
        assert_eq!(info.database, "my-db");
    }

    #[test]
    fn query_parameters_become_settings() {
        let info = parse_clickhouse_url(
            "http://localhost:8123/db?max_execution_time=600&mutations_sync=2",
        )
        .unwrap();
        assert_eq!(info.database, "db");
        assert_eq!(
            info.settings,
            vec![
                ("max_execution_time".to_string(), "600".to_string()),
                ("mutations_sync".to_string(), "2".to_string()),
            ]
        );
    }

    #[test]
    fn rejects_unknown_scheme() {
        assert!(parse_clickhouse_url("postgres://localhost/db").is_err());
    }

    #[test]
    fn quotes_string_literals() {
        assert_eq!(quote_string("abc"), "'abc'");
        assert_eq!(quote_string("it's"), "'it\\'s'");
        assert_eq!(quote_string("a\\b"), "'a\\\\b'");
    }
}
