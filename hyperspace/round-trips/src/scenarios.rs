// Included once per SQLx build (`stock`, `fork`), with `sqlx` naming that build.

use sqlx::encode::IsNull;
use sqlx::error::BoxDynError;
use sqlx::postgres::{
    PgArgumentBuffer, PgConnectOptions, PgConnection, PgPool, PgTypeInfo, PgTypeKind, PgValueRef,
    Postgres,
};
use sqlx::{Connection, Decode, Encode, Row, Type};

/// A Postgres enum value bound by type name, as a `#[derive(sqlx::Type)]` enum is.
pub struct Label(pub &'static str, pub &'static str);

impl Type<Postgres> for Label {
    fn type_info() -> PgTypeInfo {
        PgTypeInfo::with_name("text")
    }
}

impl Encode<'_, Postgres> for Label {
    fn encode_by_ref(&self, buf: &mut PgArgumentBuffer) -> Result<IsNull, BoxDynError> {
        <&str as Encode<Postgres>>::encode_by_ref(&self.1, buf)
    }

    fn produces(&self) -> Option<PgTypeInfo> {
        Some(PgTypeInfo::with_name(self.0))
    }
}

/// Decodes only from a resolved enum type, as a derived enum does.
pub struct Decoded(pub String);

impl Type<Postgres> for Decoded {
    fn type_info() -> PgTypeInfo {
        PgTypeInfo::with_name("text")
    }

    fn compatible(ty: &PgTypeInfo) -> bool {
        matches!(ty.kind(), PgTypeKind::Enum(_))
    }
}

impl<'r> Decode<'r, Postgres> for Decoded {
    fn decode(value: PgValueRef<'r>) -> Result<Self, BoxDynError> {
        Ok(Self(<String as Decode<Postgres>>::decode(value)?))
    }
}

pub async fn connect(options: &PgConnectOptions) -> anyhow::Result<PgConnection> {
    Ok(PgConnection::connect_with(options).await?)
}

pub async fn setup(options: &PgConnectOptions) -> anyhow::Result<()> {
    let mut conn = connect(options).await?;
    sqlx::raw_sql(
        "DROP TABLE IF EXISTS bench_tweet, bench_item;
         DROP TYPE IF EXISTS bench_e1, bench_e2, bench_e3, bench_e4, bench_e5;
         CREATE TYPE bench_e1 AS ENUM ('a', 'b');
         CREATE TYPE bench_e2 AS ENUM ('a', 'b');
         CREATE TYPE bench_e3 AS ENUM ('a', 'b');
         CREATE TYPE bench_e4 AS ENUM ('a', 'b');
         CREATE TYPE bench_e5 AS ENUM ('a', 'b');
         CREATE TABLE bench_tweet (id int8 PRIMARY KEY GENERATED ALWAYS AS IDENTITY, text text NOT NULL);
         CREATE TABLE bench_item (id int8 PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
             e1 bench_e1 NOT NULL, e2 bench_e2 NOT NULL, e3 bench_e3 NOT NULL,
             e4 bench_e4 NOT NULL, e5 bench_e5 NOT NULL);
         INSERT INTO bench_tweet (text) VALUES ('hello');
         INSERT INTO bench_item (e1, e2, e3, e4, e5) VALUES ('a', 'b', 'a', 'b', 'a');",
    )
    .execute(&mut conn)
    .await?;
    conn.close().await?;
    Ok(())
}

/// (a) a simple `SELECT` by id
pub async fn select_by_id(conn: &mut PgConnection, persistent: bool) -> anyhow::Result<()> {
    let row = sqlx::query("SELECT id, text FROM bench_tweet WHERE id = $1")
        .bind(1_i64)
        .persistent(persistent)
        .fetch_one(conn)
        .await?;
    anyhow::ensure!(row.try_get::<String, _>("text")? == "hello");
    Ok(())
}

/// (b) five enum parameters and five enum columns
pub async fn select_enums(conn: &mut PgConnection, persistent: bool) -> anyhow::Result<()> {
    let row = sqlx::query(
        "SELECT e1, e2, e3, e4, e5 FROM bench_item
         WHERE e1 = $1 AND e2 = $2 AND e3 = $3 AND e4 = $4 AND e5 = $5",
    )
    .bind(Label("bench_e1", "a"))
    .bind(Label("bench_e2", "b"))
    .bind(Label("bench_e3", "a"))
    .bind(Label("bench_e4", "b"))
    .bind(Label("bench_e5", "a"))
    .persistent(persistent)
    .fetch_one(conn)
    .await?;

    for (i, expected) in ["a", "b", "a", "b", "a"].into_iter().enumerate() {
        anyhow::ensure!(row.try_get::<Decoded, _>(i)?.0 == expected);
    }
    Ok(())
}

/// (c) a transaction with three statements
pub async fn transaction(conn: &mut PgConnection, persistent: bool) -> anyhow::Result<()> {
    let mut tx = conn.begin().await?;
    let id: i64 = sqlx::query_scalar("INSERT INTO bench_tweet (text) VALUES ($1) RETURNING id")
        .bind("new")
        .persistent(persistent)
        .fetch_one(&mut *tx)
        .await?;
    sqlx::query("UPDATE bench_tweet SET text = $2 WHERE id = $1")
        .bind(id)
        .bind("updated")
        .persistent(persistent)
        .execute(&mut *tx)
        .await?;
    let text: String = sqlx::query_scalar("SELECT text FROM bench_tweet WHERE id = $1")
        .bind(id)
        .persistent(persistent)
        .fetch_one(&mut *tx)
        .await?;
    anyhow::ensure!(text == "updated");
    tx.commit().await?;
    Ok(())
}

/// (d) a pooled request: acquire, (a), release
pub async fn pooled(pool: &PgPool, persistent: bool) -> anyhow::Result<()> {
    let mut conn = pool.acquire().await?;
    select_by_id(&mut conn, persistent).await
}

/// Clients outnumbering the proxy's server connections, each running its own statements;
/// returns the number of failed queries.
pub async fn concurrent(options: &PgConnectOptions, persistent: bool) -> anyhow::Result<usize> {
    let mut clients = Vec::new();

    for client in 0..8_i32 {
        let options = options.clone();
        clients.push(tokio::spawn(async move {
            let mut conn = connect(&options).await?;
            let mut failed = 0;

            for i in 0..200_i32 {
                let sql = format!("SELECT $1::int4 * 1000 + {client}");
                let result: Result<i32, _> = sqlx::query_scalar(sqlx::AssertSqlSafe(sql))
                    .bind(i)
                    .persistent(persistent)
                    .fetch_one(&mut conn)
                    .await;

                match result {
                    Ok(n) if n == i * 1000 + client => {}
                    Ok(n) => {
                        failed += 1;
                        if failed == 1 && client == 0 {
                            println!("  e.g. wrong result {n}, expected {}", i * 1000 + client);
                        }
                    }
                    Err(error) => {
                        failed += 1;
                        if failed == 1 && client == 0 {
                            println!("  e.g. {error}");
                        }
                    }
                }
            }

            anyhow::Ok(failed)
        }));
    }

    let mut failed = 0;
    for client in clients {
        failed += client.await??;
    }
    Ok(failed)
}

pub fn pool_options() -> sqlx::postgres::PgPoolOptions {
    sqlx::postgres::PgPoolOptions::new().max_connections(1)
}
