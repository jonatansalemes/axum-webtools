use scoped_futures::ScopedBoxFuture;
use sqlx::{Database, Pool, Transaction};

pub async fn with_tx<'p, F, R, E, DB>(pool: &Pool<DB>, callback: F) -> Result<R, E>
where
    F: for<'r> FnOnce(&'r mut Transaction<DB>) -> ScopedBoxFuture<'p, 'r, Result<R, E>> + Send + 'p,
    E: From<sqlx::Error> + Send + 'p,
    R: Send + 'p,
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
