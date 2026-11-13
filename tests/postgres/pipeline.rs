//! A query is one round trip: `Parse`, `Describe`, `Bind`, `Execute` and `Sync` in one write.
//!
//! Round trips are counted by a TCP proxy between the connection and the server: one each
//! time the client writes after the server has answered.
#![cfg(feature = "_rt-tokio")]

use futures_util::{StreamExt, TryStreamExt};
use sqlx::postgres::{
    PgConnectOptions, PgConnection, PgDatabaseError, PgPoolOptions, PgRow, PgTypeCache,
};
use sqlx::{Column, Connection, Executor, Row, SqlSafeStr, Statement, TypeInfo};
use sqlx_test::setup_if_needed;
use std::env;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

#[derive(Debug, PartialEq, sqlx::Type)]
#[sqlx(type_name = "status", rename_all = "lowercase")]
enum Status {
    New,
    Open,
    Closed,
}

/// Forwards connections to the server of `DATABASE_URL` and counts their round trips.
struct Proxy {
    options: PgConnectOptions,
    round_trips: Arc<AtomicUsize>,
}

impl Proxy {
    async fn start() -> anyhow::Result<Self> {
        setup_if_needed();

        let options: PgConnectOptions = env::var("DATABASE_URL")?.parse()?;
        let server = format!("{}:{}", options.get_host(), options.get_port());
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let port = listener.local_addr()?.port();
        let round_trips = Arc::new(AtomicUsize::new(0));

        let counter = round_trips.clone();
        tokio::spawn(async move {
            while let Ok((client, _)) = listener.accept().await {
                let Ok(server) = TcpStream::connect(&server).await else {
                    return;
                };
                let _ = client.set_nodelay(true);
                let _ = server.set_nodelay(true);

                // a write after a reply starts a round trip
                let answered = Arc::new(AtomicBool::new(true));
                let (client_read, client_write) = client.into_split();
                let (server_read, server_write) = server.into_split();

                tokio::spawn(pump(client_read, server_write, {
                    let answered = answered.clone();
                    let counter = counter.clone();
                    move || {
                        if answered.swap(false, Ordering::SeqCst) {
                            counter.fetch_add(1, Ordering::SeqCst);
                        }
                    }
                }));
                tokio::spawn(pump(server_read, client_write, move || {
                    answered.store(true, Ordering::SeqCst);
                }));
            }
        });

        Ok(Self {
            options: options.host("127.0.0.1").port(port),
            round_trips,
        })
    }

    async fn connect(&self) -> anyhow::Result<PgConnection> {
        Ok(PgConnection::connect_with(&self.options).await?)
    }

    /// Round trips since the last call.
    fn take(&self) -> usize {
        self.round_trips.swap(0, Ordering::SeqCst)
    }
}

async fn pump(
    mut from: tokio::net::tcp::OwnedReadHalf,
    mut to: tokio::net::tcp::OwnedWriteHalf,
    on_data: impl Fn(),
) {
    let mut buf = vec![0; 64 * 1024];

    while let Ok(n @ 1..) = from.read(&mut buf).await {
        on_data();

        if to.write_all(&buf[..n]).await.is_err() {
            return;
        }
    }

    let _ = to.shutdown().await;
}

async fn prepared_statements(conn: &mut PgConnection) -> anyhow::Result<i64> {
    Ok(
        sqlx::raw_sql("SELECT count(*)::int8 FROM pg_prepared_statements WHERE from_sql = false")
            .fetch_one(conn)
            .await?
            .get(0),
    )
}

#[sqlx_macros::test]
async fn it_runs_a_query_in_one_round_trip() -> anyhow::Result<()> {
    let proxy = Proxy::start().await?;
    let mut conn = proxy.connect().await?;
    proxy.take();

    // unnamed
    let n: i32 = sqlx::query_scalar("SELECT $1::int4 + 1")
        .bind(1_i32)
        .persistent(false)
        .fetch_one(&mut conn)
        .await?;
    assert_eq!(n, 2);
    assert_eq!(proxy.take(), 1);
    assert_eq!(conn.cached_statements_size(), 0);

    // named, prepared and cached on its first run
    for i in 0..3_i32 {
        let n: i32 = sqlx::query_scalar("SELECT $1::int4 * 2")
            .bind(i)
            .fetch_one(&mut conn)
            .await?;
        assert_eq!(n, i * 2);
        assert_eq!(proxy.take(), 1, "run {i}");
    }
    assert_eq!(conn.cached_statements_size(), 1);
    assert_eq!(prepared_statements(&mut conn).await?, 1);

    Ok(())
}

#[sqlx_macros::test]
async fn it_runs_a_transaction_in_one_round_trip_per_statement() -> anyhow::Result<()> {
    let proxy = Proxy::start().await?;
    let mut conn = proxy.connect().await?;
    sqlx::raw_sql("CREATE TEMPORARY TABLE pipeline_tx (id int4 PRIMARY KEY, label text)")
        .execute(&mut conn)
        .await?;
    proxy.take();

    let mut tx = conn.begin().await?;
    sqlx::query("INSERT INTO pipeline_tx (id, label) VALUES ($1, $2)")
        .bind(1_i32)
        .bind("one")
        .execute(&mut *tx)
        .await?;
    sqlx::query("UPDATE pipeline_tx SET label = $2 WHERE id = $1")
        .bind(1_i32)
        .bind("uno")
        .execute(&mut *tx)
        .await?;
    let label: String = sqlx::query_scalar("SELECT label FROM pipeline_tx WHERE id = $1")
        .bind(1_i32)
        .fetch_one(&mut *tx)
        .await?;
    tx.commit().await?;

    assert_eq!(label, "uno");
    // BEGIN, three statements, COMMIT
    assert_eq!(proxy.take(), 5);

    Ok(())
}

#[sqlx_macros::test]
async fn it_returns_no_rows() -> anyhow::Result<()> {
    let mut conn = Proxy::start().await?.connect().await?;

    let row = sqlx::query("SELECT $1::int4 WHERE false")
        .bind(1_i32)
        .fetch_optional(&mut conn)
        .await?;
    assert!(row.is_none());

    let rows = sqlx::query("SELECT $1::int4 WHERE false")
        .bind(1_i32)
        .fetch_all(&mut conn)
        .await?;
    assert!(rows.is_empty());

    // a statement without a row description (`NoData`)
    for persistent in [true, false] {
        let done = sqlx::query("SET application_name = 'pipeline'")
            .persistent(persistent)
            .execute(&mut conn)
            .await?;
        assert_eq!(done.rows_affected(), 0);
    }

    Ok(())
}

#[sqlx_macros::test]
async fn it_recovers_from_a_parse_error() -> anyhow::Result<()> {
    let proxy = Proxy::start().await?;
    let mut conn = proxy.connect().await?;

    for persistent in [true, false] {
        let error = sqlx::query("SELEC $1::int4")
            .bind(1_i32)
            .persistent(persistent)
            .fetch_one(&mut conn)
            .await
            .unwrap_err();
        let error = error
            .as_database_error()
            .unwrap()
            .downcast_ref::<PgDatabaseError>();
        assert_eq!(error.code(), "42601");

        let n: i32 = sqlx::query_scalar("SELECT $1::int4")
            .bind(7_i32)
            .persistent(persistent)
            .fetch_one(&mut conn)
            .await?;
        assert_eq!(n, 7);
    }

    // only the good statement was cached, and nothing else is left on the server
    assert_eq!(conn.cached_statements_size(), 1);
    assert_eq!(prepared_statements(&mut conn).await?, 1);

    Ok(())
}

#[sqlx_macros::test]
async fn it_recovers_from_an_execute_error() -> anyhow::Result<()> {
    let mut conn = Proxy::start().await?.connect().await?;

    // parsed and described, then fails while running: the statement exists, so it is cached
    let error = sqlx::query("SELECT 1 / $1::int4")
        .bind(0_i32)
        .fetch_one(&mut conn)
        .await
        .unwrap_err();
    assert_eq!(error.as_database_error().unwrap().code().unwrap(), "22012");
    assert_eq!(conn.cached_statements_size(), 1);

    let n: i32 = sqlx::query_scalar("SELECT 1 / $1::int4")
        .bind(1_i32)
        .fetch_one(&mut conn)
        .await?;
    assert_eq!(n, 1);
    assert_eq!(prepared_statements(&mut conn).await?, 1);

    // inside a transaction, the error aborts it until rolled back
    let mut tx = conn.begin().await?;
    let _ = sqlx::query("SELECT 1 / $1::int4")
        .bind(0_i32)
        .fetch_one(&mut *tx)
        .await
        .unwrap_err();
    let error = sqlx::query("SELECT $1::int4")
        .bind(1_i32)
        .fetch_one(&mut *tx)
        .await
        .unwrap_err();
    assert_eq!(error.as_database_error().unwrap().code().unwrap(), "25P02");
    tx.rollback().await?;

    let n: i32 = sqlx::query_scalar("SELECT $1::int4")
        .bind(3_i32)
        .fetch_one(&mut conn)
        .await?;
    assert_eq!(n, 3);

    Ok(())
}

#[sqlx_macros::test]
async fn it_streams_rows() -> anyhow::Result<()> {
    let mut conn = Proxy::start().await?.connect().await?;

    let mut s = sqlx::query("SELECT generate_series(1, $1::int4) AS n")
        .bind(1000_i32)
        .fetch(&mut conn);
    let mut seen = 0;
    while let Some(row) = s.try_next().await? {
        seen += 1;
        assert_eq!(row.get::<i32, _>("n"), seen);
    }
    drop(s);
    assert_eq!(seen, 1000);

    // dropped part-way: the rest is drained by the next query
    let mut s = sqlx::query("SELECT generate_series(1, $1::int4) AS n")
        .bind(1000_i32)
        .fetch(&mut conn);
    let first: PgRow = s.try_next().await?.unwrap();
    assert_eq!(first.get::<i32, _>(0), 1);
    drop(s);

    let n: i32 = sqlx::query_scalar("SELECT $1::int4")
        .bind(5_i32)
        .fetch_one(&mut conn)
        .await?;
    assert_eq!(n, 5);

    // `fetch_many` sees the result after the rows
    let results: Vec<_> = (&mut conn)
        .fetch_many(sqlx::query("SELECT generate_series(1, $1::int4)").bind(3_i32))
        .try_collect()
        .await?;
    assert_eq!(results.len(), 4);
    assert!(results[3].is_left());

    Ok(())
}

#[sqlx_macros::test]
async fn it_closes_a_statement_dropped_before_it_was_described() -> anyhow::Result<()> {
    let mut conn = Proxy::start().await?.connect().await?;

    for i in 0..10_i32 {
        let sql = format!("SELECT $1::int4 + {i}");
        let mut s = sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind(i)
            .fetch(&mut conn);
        // sends the query; the reply (through the proxy) is not there yet
        let _ = tokio::time::timeout(Duration::ZERO, s.next()).await;
        drop(s);
    }

    // statements left to close go out with the next query
    sqlx::query("SELECT 1").execute(&mut conn).await?;

    let cached = conn.cached_statements_size() as i64;
    let open = prepared_statements(&mut conn).await?;
    assert_eq!(open, cached);

    Ok(())
}

#[sqlx_macros::test]
async fn it_resolves_a_custom_type_after_the_query() -> anyhow::Result<()> {
    let proxy = Proxy::start().await?;

    for persistent in [true, false] {
        let mut conn = proxy.connect().await?;
        proxy.take();

        // the column type is only known once the rows arrive; it is looked up afterwards
        let rows = sqlx::query("SELECT $1::status AS status FROM generate_series(1, 3)")
            .bind("open")
            .persistent(persistent)
            .fetch_all(&mut conn)
            .await?;
        assert_eq!(proxy.take(), 2);
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].column(0).type_info().name(), "status");
        assert_eq!(rows[2].try_get::<Status, _>("status")?, Status::Open);

        // now it is known: one round trip
        let status: Status = sqlx::query_scalar("SELECT $1::status")
            .bind(Status::Closed)
            .persistent(persistent)
            .fetch_one(&mut conn)
            .await?;
        assert_eq!(status, Status::Closed);
        assert_eq!(proxy.take(), 1);

        assert_eq!(
            conn.cached_statements_size(),
            if persistent { 2 } else { 0 }
        );
        assert_eq!(
            prepared_statements(&mut conn).await?,
            if persistent { 2 } else { 0 }
        );
    }

    Ok(())
}

#[sqlx_macros::test]
async fn it_binds_a_custom_type_on_a_fresh_connection() -> anyhow::Result<()> {
    // https://github.com/launchbadge/sqlx/issues/4361: resolving the type of a bind parameter is
    // a query, which destroyed the unnamed statement parsed before it
    for persistent in [true, false] {
        let mut conn = Proxy::start().await?.connect().await?;

        let status: Status = sqlx::query_scalar("SELECT $1")
            .bind(Status::New)
            .persistent(persistent)
            .fetch_one(&mut conn)
            .await?;
        assert_eq!(status, Status::New);
    }

    Ok(())
}

#[sqlx_macros::test]
async fn it_closes_statements_evicted_from_the_cache() -> anyhow::Result<()> {
    setup_if_needed();
    let options: PgConnectOptions = env::var("DATABASE_URL")?.parse()?;
    let mut conn = PgConnection::connect_with(&options.statement_cache_capacity(2)).await?;

    for i in 0..10_i32 {
        let n: i32 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!("SELECT $1::int4 + {i}")))
            .bind(1_i32)
            .fetch_one(&mut conn)
            .await?;
        assert_eq!(n, 1 + i);
    }

    // statements left to close go out with the next query
    sqlx::query("SELECT $1::int4 + 9")
        .bind(1_i32)
        .execute(&mut conn)
        .await?;

    assert_eq!(conn.cached_statements_size(), 2);
    assert_eq!(prepared_statements(&mut conn).await?, 2);

    conn.clear_cached_statements().await?;
    assert_eq!(prepared_statements(&mut conn).await?, 0);

    Ok(())
}

#[sqlx_macros::test]
async fn it_names_no_statement_with_the_cache_disabled() -> anyhow::Result<()> {
    setup_if_needed();
    let options: PgConnectOptions = env::var("DATABASE_URL")?.parse()?;
    let mut conn = PgConnection::connect_with(&options.statement_cache_capacity(0)).await?;

    for i in 0..5_i32 {
        let n: i32 = sqlx::query_scalar("SELECT $1::int4")
            .bind(i)
            .fetch_one(&mut conn)
            .await?;
        assert_eq!(n, i);
    }

    let statement = conn.prepare("SELECT $1::int4 AS n".into_sql_str()).await?;
    let row = statement.query().bind(9_i32).fetch_one(&mut conn).await?;
    assert_eq!(row.get::<i32, _>("n"), 9);

    assert_eq!(prepared_statements(&mut conn).await?, 0);

    Ok(())
}

#[sqlx_macros::test]
async fn it_reuses_a_prepared_statement_object() -> anyhow::Result<()> {
    let proxy = Proxy::start().await?;
    let mut conn = proxy.connect().await?;

    let statement = conn
        .prepare("SELECT $1::int4 + 1 AS n".into_sql_str())
        .await?;

    // on another connection the statement object is parsed again, without `Describe`
    let mut other = proxy.connect().await?;
    proxy.take();
    for i in 0..2_i32 {
        let row = statement.query().bind(i).fetch_one(&mut other).await?;
        assert_eq!(row.get::<i32, _>("n"), i + 1);
    }
    assert_eq!(proxy.take(), 2);
    assert_eq!(other.cached_statements_size(), 1);
    assert_eq!(prepared_statements(&mut other).await?, 1);

    Ok(())
}

#[sqlx_macros::test]
async fn it_shares_resolved_types_between_connections() -> anyhow::Result<()> {
    let proxy = Proxy::start().await?;
    let cache = PgTypeCache::new();
    let shared = proxy.options.clone().shared_type_cache(cache.clone());

    // an enum parameter, and enum, composite, range and array columns
    async fn run(conn: &mut PgConnection) -> anyhow::Result<Vec<String>> {
        let row = sqlx::query(
            "SELECT $1 AS status, ROW('pen', 1, 2)::inventory_item AS item, \
             '[1.5,2.5]'::float_range AS range, ARRAY[$1] AS statuses",
        )
        .bind(Status::Open)
        .fetch_one(conn)
        .await?;

        assert_eq!(row.try_get::<Status, _>("status")?, Status::Open);

        Ok(row
            .columns()
            .iter()
            .map(|column| column.type_info().name().to_owned())
            .collect())
    }

    // the first connection looks the types up
    let mut first = PgConnection::connect_with(&shared).await?;
    proxy.take();
    let types = run(&mut first).await?;
    assert!(proxy.take() > 1);
    assert_eq!(
        types,
        ["status", "inventory_item", "float_range", "status[]"]
    );

    // a new connection sharing them does not
    let mut second = PgConnection::connect_with(&shared).await?;
    proxy.take();
    assert_eq!(run(&mut second).await?, types);
    assert_eq!(proxy.take(), 1);

    // nor a new pool
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(shared.clone())
        .await?;
    let mut pooled = pool.acquire().await?;
    proxy.take();
    assert_eq!(run(&mut pooled).await?, types);
    assert_eq!(proxy.take(), 1);
    drop(pooled);
    pool.close().await;

    // without the cache, a new connection looks them up again
    let mut alone = proxy.connect().await?;
    proxy.take();
    assert_eq!(run(&mut alone).await?, types);
    assert!(proxy.take() > 1);

    // once cleared, they are looked up again
    cache.clear();
    let mut third = PgConnection::connect_with(&shared).await?;
    proxy.take();
    assert_eq!(run(&mut third).await?, types);
    assert!(proxy.take() > 1);

    Ok(())
}

#[sqlx_macros::test]
async fn it_skips_the_pings_of_a_pool() -> anyhow::Result<()> {
    let proxy = Proxy::start().await?;

    for (test_before_acquire, test_on_release, per_use) in
        [(true, true, 3), (false, true, 2), (false, false, 1)]
    {
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .test_before_acquire(test_before_acquire)
            .test_on_release(test_on_release)
            .connect_with(proxy.options.clone())
            .await?;

        // the connection exists and its statement is cached
        let mut conn = pool.acquire().await?;
        sqlx::query("SELECT $1::int4")
            .bind(0_i32)
            .execute(&mut *conn)
            .await?;
        drop(conn);
        // waits for that release
        let conn = pool.acquire().await?;
        proxy.take();
        drop(conn);

        for i in 1..=5_i32 {
            let mut conn = pool.acquire().await?;
            let n: i32 = sqlx::query_scalar("SELECT $1::int4")
                .bind(i)
                .fetch_one(&mut *conn)
                .await?;
            assert_eq!(n, i);
        }

        // waits for the last release
        let conn = pool.acquire().await?;
        // the first release, five uses, the last acquire
        let expected =
            usize::from(test_on_release) + 5 * per_use + usize::from(test_before_acquire);
        assert_eq!(
            proxy.take(),
            expected,
            "test_before_acquire({test_before_acquire}), test_on_release({test_on_release})"
        );
        drop(conn);
        pool.close().await;
    }

    Ok(())
}

#[sqlx_macros::test]
async fn it_rolls_back_a_dropped_transaction_without_a_ping() -> anyhow::Result<()> {
    let proxy = Proxy::start().await?;
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .test_before_acquire(false)
        .test_on_release(false)
        .connect_with(proxy.options.clone())
        .await?;

    let mut conn = pool.acquire().await?;
    sqlx::raw_sql("CREATE TEMPORARY TABLE pipeline_release (id int4)")
        .execute(&mut *conn)
        .await?;
    let mut tx = conn.begin().await?;
    sqlx::query("INSERT INTO pipeline_release VALUES ($1)")
        .bind(1_i32)
        .execute(&mut *tx)
        .await?;
    drop(tx);
    drop(conn);

    let mut conn = pool.acquire().await?;
    let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM pipeline_release")
        .fetch_one(&mut *conn)
        .await?;
    assert_eq!(rows, 0);

    // a stream dropped part-way is drained on release
    let mut s = sqlx::query("SELECT generate_series(1, $1::int4)")
        .bind(10_000_i32)
        .fetch(&mut *conn);
    s.try_next().await?;
    drop(s);
    drop(conn);

    let mut conn = pool.acquire().await?;
    let n: i32 = sqlx::query_scalar("SELECT $1::int4")
        .bind(2_i32)
        .fetch_one(&mut *conn)
        .await?;
    assert_eq!(n, 2);

    Ok(())
}
