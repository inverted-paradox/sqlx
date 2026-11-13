//! A TCP proxy that counts round trips and adds latency.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::time::Instant;

pub struct Proxy {
    pub port: u16,
    round_trips: Arc<AtomicUsize>,
}

impl Proxy {
    /// Forwards to `server`, delaying every byte by `one_way` in each direction.
    pub async fn start(server: String, one_way: Duration) -> anyhow::Result<Self> {
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

                pump(client_read, server_write, one_way, {
                    let answered = answered.clone();
                    let counter = counter.clone();
                    move || {
                        if answered.swap(false, Ordering::SeqCst) {
                            counter.fetch_add(1, Ordering::SeqCst);
                        }
                    }
                });
                pump(server_read, client_write, one_way, move || {
                    answered.store(true, Ordering::SeqCst);
                });
            }
        });

        Ok(Self { port, round_trips })
    }

    /// Round trips since the last call.
    pub fn take(&self) -> usize {
        self.round_trips.swap(0, Ordering::SeqCst)
    }
}

fn pump(
    mut from: OwnedReadHalf,
    mut to: OwnedWriteHalf,
    delay: Duration,
    on_data: impl Fn() + Send + 'static,
) {
    let (tx, mut rx) = mpsc::unbounded_channel::<(Instant, Vec<u8>)>();

    tokio::spawn(async move {
        let mut buf = vec![0; 64 * 1024];
        while let Ok(n @ 1..) = from.read(&mut buf).await {
            on_data();
            if tx
                .send((Instant::now() + delay, buf[..n].to_vec()))
                .is_err()
            {
                return;
            }
        }
    });

    tokio::spawn(async move {
        while let Some((at, bytes)) = rx.recv().await {
            // the timer has millisecond ticks: no sleep unless there is time to wait
            if at > Instant::now() {
                tokio::time::sleep_until(at).await;
            }
            if to.write_all(&bytes).await.is_err() {
                return;
            }
        }
        let _ = to.shutdown().await;
    });
}
