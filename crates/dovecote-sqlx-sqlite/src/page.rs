//! SQLite live and finite snapshot paging.

use crate::{
    begin_read, commit_transaction,
    error::PageError,
    hydrate::{DurableRow, hydrate_page},
    install_foreign_keys,
};
use dovecote::{Limit, PagedEvent, RowId};
use sqlx::{Sqlite, SqlitePool, Transaction, query_as, query_scalar};
use std::marker::PhantomData;

/// Reads one independent live page. Separate calls do not share a snapshot.
pub async fn page(
    pool: &SqlitePool,
    after_row_id: Option<RowId>,
    limit: Limit,
) -> Result<Vec<PagedEvent>, PageError> {
    let mut connection = pool
        .acquire()
        .await
        .map_err(|source| PageError::sql("acquire live page connection", source))?;
    install_foreign_keys(&mut connection)
        .await
        .map_err(|source| PageError::sql("enable live-page foreign keys", source))?;
    read_page(
        &mut *connection,
        after_row_id.map_or(0, RowId::get),
        None,
        limit,
    )
    .await
}

/// Begins one finite read transaction and records its row-id ceiling only
/// after the snapshot has been established.
pub async fn begin_snapshot(pool: &SqlitePool) -> Result<SnapshotPager, PageError> {
    let mut transaction = begin_read(pool)
        .await
        .map_err(|source| PageError::sql("begin snapshot transaction", source))?;
    let upper_bound =
        match query_scalar::<_, Option<i64>>("SELECT MAX(row_id) FROM dovecote_events")
            .fetch_one(&mut *transaction)
            .await
        {
            Ok(value) => value,
            Err(source) => {
                let _ = transaction.rollback().await;
                return Err(PageError::sql("read snapshot upper row ID", source));
            }
        };
    let upper_bound = match upper_bound
        .map(|value| RowId::new(value).map_err(|error| PageError::serialization(error.to_string())))
        .transpose()
    {
        Ok(value) => value,
        Err(error) => {
            let _ = transaction.rollback().await;
            return Err(error);
        }
    };
    Ok(SnapshotPager {
        transaction: Some(transaction),
        upper_bound,
        cursor: None,
        exhausted: upper_bound.is_none(),
        _not_send: PhantomData,
    })
}

/// A finite pager retaining one SQLite read transaction. The explicit marker
/// makes accidental movement across unrelated executors a compile-time error.
///
/// ```compile_fail
/// use dovecote_sqlx_sqlite::SnapshotPager;
///
/// fn requires_send<T: Send>() {}
///
/// fn main() {
///     requires_send::<SnapshotPager>();
/// }
/// ```
pub struct SnapshotPager {
    transaction: Option<Transaction<'static, Sqlite>>,
    upper_bound: Option<RowId>,
    cursor: Option<RowId>,
    exhausted: bool,
    _not_send: PhantomData<*mut ()>,
}

impl SnapshotPager {
    pub const fn cursor(&self) -> Option<RowId> {
        self.cursor
    }
    pub const fn upper_bound(&self) -> Option<RowId> {
        self.upper_bound
    }
    pub const fn is_exhausted(&self) -> bool {
        self.exhausted
    }

    pub async fn next_page(&mut self, limit: Limit) -> Result<Vec<PagedEvent>, PageError> {
        if self.exhausted {
            return Ok(Vec::new());
        }

        let transaction = self.transaction.as_mut().ok_or(PageError::Closed)?;
        let upper = self
            .upper_bound
            .expect("non-exhausted pager has an upper bound");
        let result = read_page(
            &mut **transaction,
            self.cursor.map_or(0, RowId::get),
            Some(upper.get()),
            limit,
        )
        .await;
        let rows = match result {
            Ok(rows) => rows,
            Err(error) => {
                if let Some(transaction) = self.transaction.take() {
                    let _ = transaction.rollback().await;
                }

                return Err(error);
            }
        };
        if let Some(last) = rows.last() {
            self.cursor = Some(last.row_id());

            if rows.len() < limit.get() as usize || self.cursor == self.upper_bound {
                self.exhausted = true;
            }
        } else {
            self.exhausted = true;
        }

        Ok(rows)
    }

    pub async fn finish(mut self) -> Result<(), PageError> {
        let Some(transaction) = self.transaction.take() else {
            return Ok(());
        };
        commit_transaction(transaction)
            .await
            .map_err(|source| PageError::sql("finish snapshot transaction", source))
    }
    pub async fn rollback(mut self) -> Result<(), PageError> {
        let Some(transaction) = self.transaction.take() else {
            return Ok(());
        };
        transaction
            .rollback()
            .await
            .map_err(|source| PageError::sql("rollback snapshot transaction", source))
    }
    pub async fn close(self) -> Result<(), PageError> {
        self.rollback().await
    }
}

async fn read_page<'c, E>(
    executor: E,
    after_row_id: i64,
    upper_bound: Option<i64>,
    limit: Limit,
) -> Result<Vec<PagedEvent>, PageError>
where
    E: sqlx::Executor<'c, Database = Sqlite>,
{
    let rows = query_as::<_, DurableRow>(PAGE_SQL)
        .bind(after_row_id)
        .bind(upper_bound)
        .bind(upper_bound)
        .bind(i64::from(limit.get()))
        .fetch_all(executor)
        .await
        .map_err(|source| PageError::sql("read event page", source))?;
    rows.into_iter()
        .map(hydrate_page)
        .collect::<Result<Vec<_>, _>>()
        .map_err(PageError::serialization)
}

const PAGE_SQL: &str = "SELECT e.row_id, e.stream, e.specversion, e.event_id, e.source, e.event_type, e.subject, e.occurred_at, e.enqueued_at, e.datacontenttype, e.dataschema, e.partitionkey, e.extensions, e.data_kind, e.data, d.state, d.available_at, d.attempts, d.claim_token, d.claimed_by, d.claim_expires_at, d.last_failure_code, d.last_failure_detail, d.delivered_at, d.quarantined_at, d.quarantine_reason FROM dovecote_events AS e LEFT JOIN dovecote_deliveries AS d ON d.event_row_id = e.row_id WHERE e.row_id > ? AND (? IS NULL OR e.row_id <= ?) ORDER BY e.row_id ASC LIMIT ?";
