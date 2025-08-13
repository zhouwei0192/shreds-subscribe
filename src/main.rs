mod shred;
mod deshred;

use tokio::signal;
use tokio::sync::broadcast;
use tracing::{error, info};

#[tokio::main]
async fn main() -> anyhow::Result<()> {

    // Specify the receiving port for the Solana shred stream
    let port = 18999;

    // Specify the account address that needs to subscribe to the transaction.
    let subscribe_account = "7ZKL8BAPfKKa6FNmds48QKFnckrcj4mkppRnsBAR2xVH".to_string();

    // setup_logging();
    tracing_subscriber::fmt().with_target(true).with_level(true).init();

    // exit channel
    let (shutdown_tx, _) = broadcast::channel::<()>(16);

    // udp server
    let (reconstruct_tx, reconstruct_rx) = crossbeam_channel::bounded(1_024);

    let udp_shutdown_rx = shutdown_tx.subscribe();
    let udp_handle = tokio::spawn(async move {
        if let Err(e) = shred::run_udp_server(port, udp_shutdown_rx,reconstruct_tx).await {
            error!("UDP server occur err: {:?}", e);
        }
    });

    // reconstruct server
    let reconstruct_shutdown = shutdown_tx.subscribe();
    let reconstruct_handle = tokio::spawn(async move {
        let _ = deshred::reconstruct_shreds_server(reconstruct_shutdown, reconstruct_rx, subscribe_account).await;
    });


    tokio::select! {
        _ = wait_for_shutdown() => {
            info!("receive exit signal, begin exit...");
        }
    }
    let _ = shutdown_tx.send(());

    udp_handle.await?;
    reconstruct_handle.await?;

    info!("All tasks have shut down.");
    Ok(())
}

pub async fn wait_for_shutdown() {
    if signal::ctrl_c().await.is_ok() {
        info!("receive SIGINT（Ctrl+C / kill -2）");
    }
}