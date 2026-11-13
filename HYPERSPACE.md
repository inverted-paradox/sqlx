# SQLx for Hyperspace

`inverted-paradox/sqlx`, branch `hyperspace`: SQLx 0.9.0 (`v0.9.0`) plus the Postgres changes
below, for Hyperspace (Jira HS-60). One commit per change; each has tests in SQLx's own suite
(`tests/postgres/pipeline.rs`).

## What changed, and why

1. **One round trip per query** (`postgres: one round trip per query`).
   Stock SQLx runs a statement it has not cached in two round trips: `Parse`/`Describe`/`Sync`,
   wait, then `Bind`/`Execute`/`Sync`. Now `Parse`, `Describe` (statement), `Bind`, `Execute`,
   `Close` (portal) and `Sync` go out in one write, named or unnamed.
   - Parameter types are the ones declared in `Parse`; they and the "hole" types are resolved
     before the first byte (a type lookup is a query of its own).
   - A named statement is cached once its description arrives, so a `Parse` error caches nothing;
     an error later (constraint, division by zero) leaves it cached, as it exists on the server.
   - A column type the connection has not resolved: the rows are held until `ReadyForQuery`, the
     type is looked up, then the rows go out fully typed (`try_get` checks unchanged). With the
     shared type cache (2) this happens once per process. Also fixes launchbadge/sqlx#4361.
   - A statement is named only if it will be cached: `statement_cache_capacity(0)` means every
     statement is unnamed (stock 0.9.0 named and leaked them; launchbadge/sqlx#4337 on `main`).
   - Statements evicted from the cache, named only for `describe`, or sent by a query dropped
     before its description are closed behind the next query's `Sync`: no round trip of their own.
   - A message that fails to encode drops the whole batch (`WriteBuffer::truncate_unflushed`).
   - `fetch`/`fetch_many` still stream; a stream dropped part-way is drained by the next query.
     The simple-query path (`raw_sql`, `COPY`) is unchanged.
2. **Shared type cache** (`PgConnectOptions::shared_type_cache(PgTypeCache)`).
   Each connection looked up every custom type it met (enums, domains, composites, ranges, arrays)
   in `pg_type`. Connections given the same `PgTypeCache` start with what the others resolved and
   add what they resolve. A pool clones its options per connection, so one cache in the options
   serves the pool; clone the `PgTypeCache` to share it wider. Keyed by host, port, socket,
   database, user and `options` (an OID is per database). `PgTypeCache::clear()` after changing
   types under a running process. Off by default.
3. **Optional pings** (`PoolOptions::test_on_release(bool)`, default `true`).
   Every release pinged the connection (a bare `Sync`). With `false`, release only brings the
   connection back to ready: queued messages (a dropped transaction's `ROLLBACK`) are sent and an
   unfinished query is drained; a ready connection costs nothing. With the existing
   `test_before_acquire(false)` too, a pooled query is one round trip.
4. **Socket hook** (feature `postgres-socket-factory`, `PgConnectOptions::socket_factory`).
   A factory supplies the byte stream of each new connection (`Socket` and `ReadBuf` re-exported
   from `sqlx::postgres`); SQLx then does TLS per `ssl_mode` and startup/auth over it. No effect
   without the feature or without a factory. For the Worker's `connect()`: the `Socket` trait is
   `Send + Sync`, which a wasm socket wrapper has to satisfy (HS-52).
5. **Transaction-pooling proxies** (PgBouncer transaction mode, Hyperdrive).
   - `statement_cache_capacity(0)`: unnamed statements only, each parsed in the round trip that
     runs it, so nothing depends on the server connection a previous transaction ran on.
   - `close_unnamed_portal(bool)`, default `true`: leave out `Close` for proxies that reject it.
   - A missing `ParseComplete` is tolerated where a statement is prepared on its own
     (`prepare`, `describe`); the pipeline never needed it.
   - PgBouncer needs `ignore_startup_parameters = extra_float_digits` (stock SQLx too).

Plus, cherry-picked with their author kept, **the maintainer's contribution** to the Aristotle fork
(`sam-rusty/sqlx`, PR #1 `fix/postgres-outer-join-nullability`, 2026-11-02): `query!` nullability
of outer joins. The planner often runs `a LEFT JOIN b` as a `Right` join hashing `a`; SQLx marked
the wrong side nullable then, and only inspected outer joins chained from the plan root. Now the
null-extended child is chosen per join type and every plan node is walked. Tests in
`tests/postgres/describe.rs`.

### Compared with the Aristotle fork (`sam-rusty/sqlx`, `aristotle-0.9`)

Read for what it changed and why; written again here. Differences:

- Pipelines named statements too (it kept the two-step flow for them), with `Describe` of the
  statement, so cached statements keep server-described parameter types.
- No `timezone(Option)` option, no `yield_now` in `acquire` (current-thread runtimes: revisit for
  the Worker), no `close_named_statements` (unnamed via capacity 0 instead), no
  `allow_missing_parse_complete` option (always tolerated).
- The type cache keeps databases apart instead of trusting one cache per database.
- The socket factory is behind a feature and returns `io::Result`.

## Tests

`DATABASE_URL` is a scratch database loaded with `tests/postgres/setup.sql`;
`PGBOUNCER_URL` (optional) is PgBouncer in transaction mode in front of it:

```sh
DATABASE_URL=postgres://localhost/sqlx_fork_test PGBOUNCER_URL=postgres://127.0.0.1:6432/sqlx_fork_test \
cargo test --no-default-features \
  --features any,postgres,postgres-socket-factory,macros,migrate,_unstable-all-types,runtime-tokio,tls-none \
  --test 'postgres*'
cargo test -p sqlx-postgres --lib --features offline,json
```

On 2026-11-13, Postgres 18.6 and PgBouncer 1.26.1 (`default_pool_size = 2`,
`max_prepared_statements = 0`): every Postgres target passes (`postgres-pipeline`: 19 tests),
as do the `sqlx-postgres` unit tests. Other runtimes and TLS backends were only compiled.

## Measurements

`hyperspace/round-trips` (its own crate) runs the same queries on stock SQLx 0.9.0 from crates.io
and on this fork, through a proxy that counts round trips (a client write after a server reply)
and adds latency:

```sh
cd hyperspace/round-trips
cargo run --release -- postgres://localhost/sqlx_fork_test postgres://127.0.0.1:6432/sqlx_fork_test 5
```

Fork settings: `shared_type_cache`, pool `test_before_acquire(false).test_on_release(false)`,
and `statement_cache_capacity(0)` through PgBouncer. Stock: defaults, and `persistent(false)`
through PgBouncer (its only way to unnamed statements). Round trips exclude connecting (1 with
`trust`). Simulated round-trip time 5 ms (one round trip costs about 8 ms through the proxy);
medians of 15 runs; "first" is the first connection of the process, before the type cache is warm.

| target | scenario | stock RTs | fork RTs | stock ms | fork ms |
|---|---|---|---|---|---|
| postgres | (a) select by id, new connection | 2 | 1 | 20.22 | 10.84 |
| postgres | (a) select by id, again | 1 | 1 | 8.65 | 8.07 |
| postgres | (b) 5 enums, new connection | 3 | 1 (first 2) | 31.67 | 11.26 |
| postgres | (b) 5 enums, again | 1 | 1 | 8.01 | 9.17 |
| postgres | (c) transaction of 3, new connection | 8 | 5 | 71.90 | 46.12 |
| postgres | (c) transaction of 3, again | 5 | 5 | 42.99 | 41.89 |
| postgres | (d) pooled select by id | 3 | 1 | 25.94 | 8.22 |
| pgbouncer | (a) select by id, new connection | 2 | 1 | 16.32 | 8.04 |
| pgbouncer | (a) select by id, again | 2 | 1 | 16.70 | 8.09 |
| pgbouncer | (b) 5 enums, new connection | 3 | 1 (first 2) | 28.13 | 9.03 |
| pgbouncer | (b) 5 enums, again | 2 | 1 | 17.53 | 8.84 |
| pgbouncer | (c) transaction of 3, new connection | 8 | 5 | 66.07 | 42.26 |
| pgbouncer | (c) transaction of 3, again | 8 | 5 | 66.89 | 42.85 |
| pgbouncer | (d) pooled select by id | 4 | 1 | 34.59 | 8.50 |

(c) is `BEGIN`, insert, update, select, `COMMIT`: 5 is the floor while `BEGIN` and `COMMIT` are
round trips of their own. Without added latency the round-trip counts are the same; wall times
are then 0.1–5 ms, noisy and dominated by the server (commit fsync).

**Correctness through PgBouncer**: 8 clients, each 200 queries of its own statement, over 2 server
connections. Stock: 1412–1505 of 1600 failed, mostly with **another client's result** (its
unnamed statement was parsed on one server connection and bound on another, which held a
different client's unnamed statement). Fork: 0 failed.

## Adopting it in Hyperspace (integration step)

```toml
[patch.crates-io]
sqlx = { git = "https://github.com/inverted-paradox/sqlx", branch = "hyperspace" }
sqlx-core = { git = "https://github.com/inverted-paradox/sqlx", branch = "hyperspace" }
sqlx-postgres = { git = "https://github.com/inverted-paradox/sqlx", branch = "hyperspace" }
sqlx-macros = { git = "https://github.com/inverted-paradox/sqlx", branch = "hyperspace" }
sqlx-macros-core = { git = "https://github.com/inverted-paradox/sqlx", branch = "hyperspace" }
```

- In the root `Cargo.toml`, not `.cargo/worker.toml`: the patch applies to every build, and the
  Worker's lockfile is derived from `Cargo.lock`. The overlay keeps only tokio, tokio-macros and mio.
- Checked on Hyperspace `a6fb85f` (tokio `=1.54.1`) with these lines: `cargo check --workspace
  --all-targets` and `just wasm` (`worker_check`, `wasm32-unknown-emscripten` with the tokio and
  mio forks) pass, with and without `postgres-socket-factory`.
- `sqlx` stays `0.9.0` with its features; add `postgres-socket-factory` only where a socket
  factory is used (the Worker, later).
- One `PgTypeCache` per process, cloned into the `PgConnectOptions` of every pool:
  `.shared_type_cache(cache.clone())`.
- Pools: `.test_on_release(false)`, and `.test_before_acquire(false)` where a dead idle connection
  is acceptable to find on use (decide per runtime; Lambda freezes may leave dead sockets).
- Behind PgBouncer or Hyperdrive: `.statement_cache_capacity(0)`; `.close_unnamed_portal(false)`
  only if the proxy rejects `Close` (untested against Hyperdrive).

## Rebasing on a new SQLx release

```sh
git fetch upstream --tags
git switch -c hyperspace-<version> hyperspace
git rebase --onto v<version> v0.9.0
```

Keep `hyperspace` as it is so existing lock files still resolve; point the `[patch]` at the new
branch. Expect conflicts in `sqlx-postgres/src/connection/executor.rs`, where `main` already has
launchbadge/sqlx#4337 (same as change 1's naming rule: keep ours), #4361 (made moot by the
pipeline: drop theirs) and #4375 (retry after a stale cached statement: keep it, around the
pipelined write). Then run the tests and the measurements above, and update this file.
