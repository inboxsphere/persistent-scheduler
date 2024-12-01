use tokio::signal;

pub(crate) async fn shutdown_signal() {
    let ctrl_c_signal = async {
        signal::ctrl_c()
            .await
            .expect("Error installing Ctrl+C signal handler");
    };

    #[cfg(unix)]
    let terminate_signal = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("Error installing terminate signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate_signal = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c_signal => {},
        _ = terminate_signal => {},
    };
}
