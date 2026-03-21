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
use tracing::{debug, error, info, warn};
use tracing_subscriber::EnvFilter;

use nexus_server::config::ServerConfig;
use nexus_server::database::{Database, DatabaseConfig};
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

    /// Enable authentication (creates default admin user on first start)
    #[arg(long, env = "NEXUS_AUTH")]
    auth: bool,

    /// Admin username (used with --auth for bootstrap)
    #[arg(long, default_value = "admin", env = "NEXUS_ADMIN_USER")]
    admin_user: String,

    /// Admin password (used with --auth for bootstrap; required when --auth is set)
    #[arg(long, env = "NEXUS_ADMIN_PASSWORD")]
    admin_password: Option<String>,

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

    // Auth configuration
    if args.auth {
        config.auth_enabled = true;
        config.admin_user = args.admin_user.clone();
        config.admin_password = args.admin_password.clone();
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
    // Build DatabaseConfig with auth settings
    let mut db_config = if config.memory_mode {
        info!("Starting in memory-only mode (data will not be persisted)");
        DatabaseConfig::in_memory()
    } else if let Some(ref dir) = config.data_dir {
        info!("Data directory: {}", dir.display());
        std::fs::create_dir_all(dir).context("Failed to create data directory")?;
        DatabaseConfig::with_path(dir.to_string_lossy().to_string())
    } else {
        info!("No data directory specified, using memory mode");
        DatabaseConfig::in_memory()
    };

    // Wire auth settings from ServerConfig → DatabaseConfig
    db_config.auth_enabled = config.auth_enabled;
    db_config.authz_enabled = config.auth_enabled; // enable RBAC when auth is on

    let db = Database::open(db_config).context("Failed to open database")?;

    // Bootstrap admin user when auth is enabled
    if config.auth_enabled {
        let password = match config.admin_password.as_deref() {
            Some(p) if !p.is_empty() => p.to_string(),
            _ => {
                // Generate a random password instead of using a hard-coded default
                let random_pw: String = (0..16)
                    .map(|_| {
                        let idx = rand::random::<u8>() % 62;
                        match idx {
                            0..=9 => (b'0' + idx) as char,
                            10..=35 => (b'a' + idx - 10) as char,
                            _ => (b'A' + idx - 36) as char,
                        }
                    })
                    .collect();
                warn!(
                    "No admin password provided — generated random password. \
                     Set NEXUS_ADMIN_PASSWORD for production!"
                );
                info!("Generated admin password: {}", random_pw);
                random_pw
            }
        };
        let authenticator = db.authenticator();
        match authenticator.create_user(&config.admin_user, &password, vec!["superuser".to_string()]) {
            Ok(_) => info!("Created admin user \"{}\"", config.admin_user),
            Err(_) => info!("Admin user \"{}\" already exists", config.admin_user),
        }
        info!("Authentication enforcement ENABLED");
    }

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
    info!("  Auth enabled: {}", config.auth_enabled);

    // Create the gRPC server
    let grpc_addr = addr;
    let grpc_server = GrpcServer::new(db.clone(), grpc_addr);

    // Create the PostgreSQL wire protocol server
    let pg_port = config.port.saturating_sub(1).max(5432);
    let pg_addr: SocketAddr = format!("{}:{}", config.host, pg_port)
        .parse()
        .context("Invalid PG wire address")?;

    let pg_factory = Arc::new(nexus_server::pgwire_handler::NexusPgWireFactory::new(
        db.clone(),
    ));

    // Start both servers with graceful shutdown
    info!("Starting gRPC server on {}...", grpc_addr);
    info!("Starting PostgreSQL wire protocol on {}...", pg_addr);
    info!("  Connect with: psql -h {} -p {}", config.host, pg_port);
    info!("Press Ctrl+C to shutdown");

    tokio::select! {
        result = grpc_server.serve() => {
            if let Err(e) = result {
                error!("gRPC server error: {}", e);
                return Err(anyhow::anyhow!("gRPC server error: {}", e));
            }
        }
        result = run_pgwire_server(pg_addr, pg_factory) => {
            if let Err(e) = result {
                error!("pgwire server error: {}", e);
                return Err(anyhow::anyhow!("pgwire server error: {}", e));
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

/// Runs the PostgreSQL wire protocol listener.
async fn run_pgwire_server(
    addr: SocketAddr,
    factory: Arc<nexus_server::pgwire_handler::NexusPgWireFactory>,
) -> Result<()> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    loop {
        let (socket, peer) = listener.accept().await?;
        debug!("pgwire connection from {}", peer);
        let f = factory.clone();
        tokio::spawn(async move {
            if let Err(e) = pgwire::tokio::process_socket(socket, None, f).await {
                error!("pgwire connection error from {}: {}", peer, e);
            }
        });
    }
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
