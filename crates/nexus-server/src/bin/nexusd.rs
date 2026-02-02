//! NexusDB Server Daemon
//!
//! The `nexusd` binary is the main NexusDB server process that:
//! - Initializes the database engine
//! - Starts the gRPC server for client connections
//! - Handles graceful shutdown on SIGTERM/SIGINT
//!
//! # Usage
//!
//! ```bash
//! # Start server with default settings
//! nexusd
//!
//! # Start with custom data directory
//! nexusd --data-dir /var/lib/nexusdb
//!
//! # Start on custom port
//! nexusd --port 5433
//!
//! # Use configuration file
//! nexusd --config /etc/nexusdb/nexusd.toml
//! ```

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use clap::Parser;
use tokio::signal;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

use nexus_server::config::ServerConfig;
use nexus_server::database::Database;
use nexus_server::grpc::GrpcServer;

/// NexusDB Server Daemon
#[derive(Parser, Debug)]
#[command(
    name = "nexusd",
    author = "NexusDB Team",
    version,
    about = "NexusDB database server",
    long_about = "NexusDB is a high-performance NewSQL database.\n\n\
                  This daemon starts the database server and listens for client connections."
)]
struct Args {
    /// Host address to bind to
    #[arg(short = 'H', long, default_value = "0.0.0.0", env = "NEXUS_HOST")]
    host: String,

    /// Port to listen on
    #[arg(short = 'p', long, default_value_t = 5432, env = "NEXUS_PORT")]
    port: u16,

    /// Data directory for persistent storage
    #[arg(short = 'd', long, value_name = "DIR", env = "NEXUS_DATA_DIR")]
    data_dir: Option<PathBuf>,

    /// Configuration file path
    #[arg(short = 'c', long, value_name = "FILE")]
    config: Option<PathBuf>,

    /// Run in memory-only mode (no persistence)
    #[arg(long)]
    memory: bool,

    /// Enable verbose logging
    #[arg(short = 'v', long)]
    verbose: bool,

    /// Log level (error, warn, info, debug, trace)
    #[arg(long, default_value = "info", env = "NEXUS_LOG_LEVEL")]
    log_level: String,

    /// Maximum number of connections
    #[arg(long, default_value_t = 100, env = "NEXUS_MAX_CONNECTIONS")]
    max_connections: usize,

    /// Buffer pool size in MB
    #[arg(long, default_value_t = 128, env = "NEXUS_BUFFER_POOL_MB")]
    buffer_pool_mb: usize,

    /// WAL directory (defaults to data_dir/wal)
    #[arg(long, value_name = "DIR", env = "NEXUS_WAL_DIR")]
    wal_dir: Option<PathBuf>,

    /// Print configuration and exit
    #[arg(long)]
    print_config: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // Initialize logging
    init_logging(&args);

    // Load configuration
    let config = load_config(&args)?;

    // Print config and exit if requested
    if args.print_config {
        println!("{}", config.to_toml()?);
        return Ok(());
    }

    // Print banner
    print_banner();

    // Initialize and start the server
    run_server(config).await
}

fn init_logging(args: &Args) {
    let level = if args.verbose {
        "debug"
    } else {
        &args.log_level
    };

    let filter = EnvFilter::try_new(format!(
        "nexus_server={level},nexus_sql={level},nexus_storage={level},nexus_txn={level}"
    ))
    .unwrap_or_else(|_| EnvFilter::new("info"));

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
        .with_thread_ids(false)
        .with_file(false)
        .with_line_number(false)
        .init();
}

fn load_config(args: &Args) -> Result<ServerConfig> {
    // Start with defaults
    let mut config = if let Some(path) = &args.config {
        ServerConfig::from_file(path).context("Failed to load config file")?
    } else {
        ServerConfig::default()
    };

    // Override with command-line arguments
    config.host = args.host.clone();
    config.port = args.port;

    if let Some(dir) = &args.data_dir {
        config.data_dir = Some(dir.clone());
    }

    if args.memory {
        config.memory_mode = true;
    }

    config.max_connections = args.max_connections;
    config.buffer_pool_mb = args.buffer_pool_mb;

    if let Some(dir) = &args.wal_dir {
        config.wal_dir = Some(dir.clone());
    }

    Ok(config)
}

fn print_banner() {
    let version = env!("CARGO_PKG_VERSION");
    info!("╔═══════════════════════════════════════════════════════╗");
    info!("║                                                       ║");
    info!("║   ███╗   ██╗███████╗██╗  ██╗██╗   ██╗███████╗         ║");
    info!("║   ████╗  ██║██╔════╝╚██╗██╔╝██║   ██║██╔════╝         ║");
    info!("║   ██╔██╗ ██║█████╗   ╚███╔╝ ██║   ██║███████╗         ║");
    info!("║   ██║╚██╗██║██╔══╝   ██╔██╗ ██║   ██║╚════██║         ║");
    info!("║   ██║ ╚████║███████╗██╔╝ ██╗╚██████╔╝███████║         ║");
    info!("║   ╚═╝  ╚═══╝╚══════╝╚═╝  ╚═╝ ╚═════╝ ╚══════╝         ║");
    info!("║                                                       ║");
    info!("║          NexusDB v{:<10} - NewSQL Database        ║", version);
    info!("║                                                       ║");
    info!("╚═══════════════════════════════════════════════════════╝");
}

async fn run_server(config: ServerConfig) -> Result<()> {
    // Create the database
    let db = if config.memory_mode {
        info!("Starting in memory-only mode (data will not be persisted)");
        Database::open_memory().context("Failed to create in-memory database")?
    } else if let Some(ref dir) = config.data_dir {
        info!("Data directory: {}", dir.display());
        
        // Ensure directory exists
        std::fs::create_dir_all(dir).context("Failed to create data directory")?;
        
        // For now, use memory mode since file-based storage needs more setup
        warn!("File-based storage not yet fully implemented, using memory mode");
        Database::open_memory().context("Failed to create database")?
    } else {
        info!("No data directory specified, using memory mode");
        Database::open_memory().context("Failed to create in-memory database")?
    };

    let db = Arc::new(db);

    // Create server address
    let addr: SocketAddr = format!("{}:{}", config.host, config.port)
        .parse()
        .context("Invalid server address")?;

    info!("Server configuration:");
    info!("  Listen address: {}", addr);
    info!("  Max connections: {}", config.max_connections);
    info!("  Buffer pool: {} MB", config.buffer_pool_mb);
    info!("  Memory mode: {}", config.memory_mode);

    // Create the gRPC server
    let grpc_server = GrpcServer::new(db.clone(), addr);

    // Start the server with graceful shutdown
    info!("Starting gRPC server on {}...", addr);
    info!("Press Ctrl+C to shutdown");

    tokio::select! {
        result = grpc_server.serve() => {
            if let Err(e) = result {
                error!("Server error: {}", e);
                return Err(anyhow::anyhow!("Server error: {}", e));
            }
        }
        _ = shutdown_signal() => {
            info!("Shutdown signal received");
        }
    }

    // Graceful shutdown
    info!("Shutting down gracefully...");
    
    // Close all sessions
    let stats = db.stats();
    if stats.active_sessions > 0 {
        warn!("Closing {} active sessions", stats.active_sessions);
    }

    info!("Server stopped. Goodbye!");
    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("Failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("Failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}
