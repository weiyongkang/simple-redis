use anyhow::Result;
use simple_redis::{network, Backend};
use tokio::net::TcpListener;
use tracing::{info, warn};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let addr = "0.0.0.0:6379";
    info!("Simple Redis Server listening on {}", addr);
    let listener = TcpListener::bind(addr).await?;

    let backend = Backend::new();
    loop {
        let (socket, raddr) = listener.accept().await?;
        info!("Accepted connection from: {}", raddr);
        let backend_clone = backend.clone();
        tokio::spawn(async move {
            match network::stream_handler(socket, backend_clone).await {
                Ok(_) => info!("Connection closed from: {}", raddr),
                Err(e) => warn!("Connection error from {}: {}", raddr, e),
            }
        });
    }
}
