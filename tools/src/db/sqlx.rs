use sqlx::{Database, Pool, Transaction};

pub async fn with_tx<R, E, DB>(
    pool: &Pool<DB>,
    callback: impl AsyncFnOnce(&mut Transaction<'_, DB>) -> Result<R, E>,
) -> Result<R, E>
where
    E: From<sqlx::Error>,
    DB: Database,
{
    let mut tx = pool.begin().await?;
    let res = callback(&mut tx).await;
    match res {
        Ok(response) => {
            tx.commit().await?;
            Ok(response)
        }
        Err(e) => {
            tx.rollback().await?;
            Err(e)
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    fn get_database_url() -> String {
        std::env::var("DATABASE_URL").unwrap_or_else(|_| {
            "postgres://pgsqlmigrate:pgsqlmigrate@pgsql:5432/pgsqlmigrate".to_string()
        })
    }

    #[tokio::test]
    async fn test_tx_commit() -> Result<(), sqlx::Error> {
        let pgsql_pool = Pool::<sqlx::Postgres>::connect(&get_database_url()).await?;
        let res = with_tx(&pgsql_pool, async |tx| -> Result<&str, sqlx::Error> {
                sqlx::query("CREATE TABLE IF NOT EXISTS test_tx_commit (id SERIAL PRIMARY KEY, name TEXT)")
                    .execute(&mut **tx)
                    .await?;
                sqlx::query("INSERT INTO test_tx_commit (name) VALUES ($1)")
                    .bind("test commit".to_string())
                    .execute(&mut **tx)
                    .await?;
                Ok("some")
        }).await;
        assert!(res.is_ok());
        assert_eq!(res.unwrap(), "some");
        Ok(())
    }

    //with rollback

    #[tokio::test]
    async fn test_tx_rollback () -> Result<(), sqlx::Error> {
        let pgsql_pool = Pool::<sqlx::Postgres>::connect(&get_database_url()).await?;
        let res: Result<bool, sqlx::Error> = with_tx(&pgsql_pool, async |tx| -> Result<bool, sqlx::Error> {
            sqlx::query("CREATE TABLE IF NOT EXISTS test_tx_commit (id SERIAL PRIMARY KEY, name TEXT)")
                .execute(&mut **tx)
                .await?;
            sqlx::query("INSERT INTO test_tx_commit (name) VALUES ($1)")
                .bind("test commit".to_string())
                .execute(&mut **tx)
                .await?;

            sqlx::query("INSERT INTO test_tx_commit (name, other) VALUES ($1,$2)")
                .bind("test commit".to_string())
                .bind("test rollback2".to_string())
                .execute(&mut **tx)
                .await?;
            Ok(true)
        }).await;
        assert!(res.is_err());
        Ok(())
    }
}
