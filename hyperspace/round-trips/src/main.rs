//! Counts round trips and wall time of the same queries on stock SQLx 0.9.0 and this fork.
//!
//! `cargo run --release -- <postgres url> [<pgbouncer url>] [<rtt ms>]`
//!
//! Every connection goes through a local proxy that counts round trips (a client write after a
//! server reply) and delays each direction by half the round-trip time.

mod proxy;

mod stock_sqlx {
    pub use ::stock as sqlx;
    include!("scenarios.rs");
}

#[allow(dead_code)]
mod fork_sqlx {
    pub use ::fork as sqlx;
    include!("scenarios.rs");
}

use proxy::Proxy;
use std::time::{Duration, Instant};

const RUNS: usize = 15;

struct Measure {
    target: &'static str,
    scenario: &'static str,
    lib: &'static str,
    first: usize,
    round_trips: f64,
    millis: f64,
}

fn median(mut values: Vec<f64>) -> f64 {
    values.sort_by(f64::total_cmp);
    values[values.len() / 2]
}

macro_rules! measure {
    ($m:ident, $lib:tt, $target:expr, $url:expr, $one_way:expr, $results:expr) => {{
        use $m::sqlx::postgres::PgConnectOptions;

        let target: &'static str = $target;
        let through_pooler = target == "pgbouncer";
        let options: PgConnectOptions = $url.parse()?;
        let proxy = Proxy::start(
            format!("{}:{}", options.get_host(), options.get_port()),
            $one_way,
        )
        .await?;
        let options = options.host("127.0.0.1").port(proxy.port);
        let (options, persistent, pool) = tune!($lib, options, through_pooler);

        // connecting
        let mut connects = Vec::new();
        for _ in 0..RUNS {
            proxy.take();
            let conn = $m::connect(&options).await?;
            connects.push(proxy.take() as f64);
            drop(conn);
        }
        $results.push(Measure {
            target,
            scenario: "connect",
            lib: $lib,
            first: connects[0] as usize,
            round_trips: median(connects),
            millis: f64::NAN,
        });

        macro_rules! scenario {
            ($name:literal, $warm:literal, $run:path) => {{
                let (mut first, mut cold, mut cold_ms, mut warm, mut warm_ms) =
                    (0, Vec::new(), Vec::new(), Vec::new(), Vec::new());

                for i in 0..RUNS {
                    let mut conn = $m::connect(&options).await?;

                    proxy.take();
                    let start = Instant::now();
                    $run(&mut conn, persistent).await?;
                    cold_ms.push(start.elapsed().as_secs_f64() * 1000.0);
                    let rts = proxy.take();
                    cold.push(rts as f64);
                    if i == 0 {
                        first = rts;
                    }

                    let start = Instant::now();
                    $run(&mut conn, persistent).await?;
                    warm_ms.push(start.elapsed().as_secs_f64() * 1000.0);
                    warm.push(proxy.take() as f64);
                }

                $results.push(Measure {
                    target,
                    scenario: $name,
                    lib: $lib,
                    first,
                    round_trips: median(cold),
                    millis: median(cold_ms),
                });
                $results.push(Measure {
                    target,
                    scenario: $warm,
                    lib: $lib,
                    first: 0,
                    round_trips: median(warm),
                    millis: median(warm_ms),
                });
            }};
        }

        scenario!(
            "(a) select by id, new connection",
            "(a) select by id, again",
            $m::select_by_id
        );
        scenario!(
            "(b) 5 enums, new connection",
            "(b) 5 enums, again",
            $m::select_enums
        );
        scenario!(
            "(c) transaction of 3, new connection",
            "(c) transaction of 3, again",
            $m::transaction
        );

        // (d) pooled requests on one warm pooled connection
        let pool = pool.connect_with(options.clone()).await?;
        $m::pooled(&pool, persistent).await?;
        drop(pool.acquire().await?);
        let requests = 50;
        let held = pool.acquire().await?;
        proxy.take();
        drop(held);
        let start = Instant::now();
        for _ in 0..requests {
            $m::pooled(&pool, persistent).await?;
        }
        // waits for the last release
        let held = pool.acquire().await?;
        let millis = start.elapsed().as_secs_f64() * 1000.0 / requests as f64;
        $results.push(Measure {
            target,
            scenario: "(d) pooled select by id",
            lib: $lib,
            first: 0,
            round_trips: proxy.take() as f64 / requests as f64,
            millis,
        });
        drop(held);
        pool.close().await;

        if through_pooler {
            let failed = $m::concurrent(&options, persistent).await?;
            println!(
                "{} through {target}: 8 clients x 200 queries, {failed} failed",
                $lib
            );
        }
    }};
}

macro_rules! tune {
    ("stock", $options:expr, $through_pooler:expr) => {{
        // stock 0.9.0 names a persistent statement even with the cache disabled: only
        // `persistent(false)` keeps statements unnamed behind a transaction pooler
        ($options, !$through_pooler, stock_sqlx::pool_options())
    }};
    ("fork", $options:expr, $through_pooler:expr) => {{
        let options = $options.shared_type_cache(::fork::postgres::PgTypeCache::new());
        let options = if $through_pooler {
            options.statement_cache_capacity(0)
        } else {
            options
        };
        let pool = fork_sqlx::pool_options()
            .test_before_acquire(false)
            .test_on_release(false);
        (options, true, pool)
    }};
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let url = args.first().expect("postgres url").clone();
    let pgbouncer = args.get(1).filter(|url| !url.is_empty()).cloned();
    let rtt: u64 = args.get(2).map_or(Ok(0), |ms| ms.parse())?;
    let one_way = Duration::from_micros(rtt * 500);

    stock_sqlx::setup(&url.parse()?).await?;

    let mut results = Vec::new();
    let mut targets = vec![("postgres", url)];
    targets.extend(pgbouncer.map(|url| ("pgbouncer", url)));

    for (target, url) in targets {
        measure!(stock_sqlx, "stock", target, url, one_way, results);
        measure!(fork_sqlx, "fork", target, url, one_way, results);
    }

    println!();
    println!("simulated round-trip time: {rtt} ms; medians of {RUNS} runs");
    println!();
    println!("| target | scenario | stock RTs | fork RTs | stock ms | fork ms |");
    println!("|---|---|---|---|---|---|");

    for stock in results.iter().filter(|m| m.lib == "stock") {
        let fork = results
            .iter()
            .find(|m| m.lib == "fork" && m.target == stock.target && m.scenario == stock.scenario)
            .unwrap();

        let rts = |m: &Measure| {
            if m.first != 0 && m.first as f64 != m.round_trips {
                format!("{} (first {})", m.round_trips, m.first)
            } else {
                format!("{}", m.round_trips)
            }
        };
        let ms = |m: &Measure| {
            if m.millis.is_nan() {
                "".to_owned()
            } else {
                format!("{:.2}", m.millis)
            }
        };

        println!(
            "| {} | {} | {} | {} | {} | {} |",
            stock.target,
            stock.scenario,
            rts(stock),
            rts(fork),
            ms(stock),
            ms(fork),
        );
    }

    Ok(())
}
