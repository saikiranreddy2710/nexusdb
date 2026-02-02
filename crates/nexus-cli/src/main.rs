//! NexusDB Command-Line Interface
//!
//! A powerful CLI for interacting with NexusDB databases.
//!
//! # Usage
//!
//! ```bash
//! # Start interactive REPL
//! nexus -H localhost -p 5432
//!
//! # Execute a single command
//! nexus -c "SELECT * FROM users"
//!
//! # Execute commands from a file
//! nexus -f queries.sql
//!
//! # Output as JSON
//! nexus -o json -c "SELECT * FROM users"
//! ```

use std::path::PathBuf;
use std::process::ExitCode;

use anyhow::Result;
use clap::{Parser, ValueEnum};
use tracing::info;
use tracing_subscriber::EnvFilter;

mod commands;
mod config;
mod formatter;
mod repl;

use config::CliConfig;
use formatter::OutputFormat;
use repl::Repl;

/// NexusDB command-line interface
#[derive(Parser, Debug)]
#[command(
    name = "nexus",
    author = "NexusDB Team",
    version,
    about = "Command-line interface for NexusDB",
    long_about = "A powerful command-line interface for interacting with NexusDB databases.\n\n\
                  Use this tool for interactive SQL sessions, executing queries from files,\n\
                  and managing your NexusDB instances."
)]
struct Args {
    /// Server hostname
    #[arg(short = 'H', long, default_value = "localhost", env = "NEXUS_HOST")]
    host: String,

    /// Server port
    #[arg(short = 'p', long, default_value_t = 5432, env = "NEXUS_PORT")]
    port: u16,

    /// Database name
    #[arg(short = 'd', long, env = "NEXUS_DATABASE")]
    database: Option<String>,

    /// Username
    #[arg(short = 'U', long, env = "NEXUS_USER")]
    user: Option<String>,

    /// Password (use NEXUS_PASSWORD env var for security)
    #[arg(short = 'W', long, env = "NEXUS_PASSWORD", hide_env_values = true)]
    password: Option<String>,

    /// Execute a single SQL command and exit
    #[arg(short = 'c', long)]
    command: Option<String>,

    /// Execute SQL commands from file and exit
    #[arg(short = 'f', long, value_name = "FILE")]
    file: Option<PathBuf>,

    /// Output format
    #[arg(short = 'o', long, value_enum, default_value = "table")]
    output: OutputFormatArg,

    /// Enable verbose output
    #[arg(short = 'v', long)]
    verbose: bool,

    /// Suppress banner and prompts (for scripting)
    #[arg(short = 'q', long)]
    quiet: bool,

    /// Configuration file path
    #[arg(long, value_name = "FILE")]
    config: Option<PathBuf>,

    /// Use mock mode (no server connection)
    #[arg(long, hide = true)]
    mock: bool,
}

/// Output format argument
#[derive(Debug, Clone, Copy, ValueEnum)]
enum OutputFormatArg {
    /// Display results in a formatted table
    Table,
    /// Display results as JSON
    Json,
    /// Display results as CSV
    Csv,
    /// Display raw values
    Raw,
}

impl From<OutputFormatArg> for OutputFormat {
    fn from(arg: OutputFormatArg) -> Self {
        match arg {
            OutputFormatArg::Table => OutputFormat::Table,
            OutputFormatArg::Json => OutputFormat::Json,
            OutputFormatArg::Csv => OutputFormat::Csv,
            OutputFormatArg::Raw => OutputFormat::Raw,
        }
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("Error: {e}");
            ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<()> {
    let args = Args::parse();

    // Initialize logging
    init_logging(args.verbose);

    // Load configuration
    let config = load_config(&args)?;

    // Determine execution mode
    if let Some(command) = &args.command {
        // Execute single command
        execute_command(&config, command, args.output.into()).await
    } else if let Some(file) = &args.file {
        // Execute from file
        execute_file(&config, file, args.output.into()).await
    } else {
        // Start interactive REPL
        run_repl(&config, args.output.into(), args.quiet).await
    }
}

fn init_logging(verbose: bool) {
    let filter = if verbose {
        EnvFilter::new("nexus_cli=debug,nexus_client=debug")
    } else {
        EnvFilter::new("nexus_cli=warn")
    };

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .without_time()
        .init();
}

fn load_config(args: &Args) -> Result<CliConfig> {
    // Try to load from config file
    let mut config = if let Some(path) = &args.config {
        CliConfig::from_file(path)?
    } else {
        CliConfig::load_default()?
    };

    // Override with command line arguments
    config.host = args.host.clone();
    config.port = args.port;

    if let Some(db) = &args.database {
        config.database = Some(db.clone());
    }
    if let Some(user) = &args.user {
        config.username = Some(user.clone());
    }
    if let Some(pass) = &args.password {
        config.password = Some(pass.clone());
    }

    config.mock_mode = args.mock;

    Ok(config)
}

async fn execute_command(config: &CliConfig, sql: &str, format: OutputFormat) -> Result<()> {
    info!("Executing command: {}", sql);

    let mut repl = Repl::new(config.clone(), format)?;
    repl.connect().await?;
    repl.execute_and_print(sql).await?;

    Ok(())
}

async fn execute_file(config: &CliConfig, path: &PathBuf, format: OutputFormat) -> Result<()> {
    info!("Executing file: {}", path.display());

    let content = std::fs::read_to_string(path)?;
    let mut repl = Repl::new(config.clone(), format)?;
    repl.connect().await?;

    // Split by semicolons and execute each statement
    for statement in split_statements(&content) {
        let trimmed = statement.trim();
        if !trimmed.is_empty() && !trimmed.starts_with("--") {
            repl.execute_and_print(trimmed).await?;
        }
    }

    Ok(())
}

async fn run_repl(config: &CliConfig, format: OutputFormat, quiet: bool) -> Result<()> {
    let mut repl = Repl::new(config.clone(), format)?;

    if !quiet {
        repl.print_banner();
    }

    repl.connect().await?;
    repl.run().await
}

/// Split SQL content into individual statements.
fn split_statements(content: &str) -> Vec<&str> {
    let mut statements = Vec::new();
    let mut start = 0;
    let mut in_string = false;
    let mut string_char = ' ';
    let mut in_comment = false;
    let mut in_block_comment = false;

    let chars: Vec<char> = content.chars().collect();
    let len = chars.len();

    let mut i = 0;
    while i < len {
        let c = chars[i];

        // Handle block comments
        if !in_string && i + 1 < len && c == '/' && chars[i + 1] == '*' {
            in_block_comment = true;
            i += 2;
            continue;
        }
        if in_block_comment && i + 1 < len && c == '*' && chars[i + 1] == '/' {
            in_block_comment = false;
            i += 2;
            continue;
        }
        if in_block_comment {
            i += 1;
            continue;
        }

        // Handle line comments
        if !in_string && i + 1 < len && c == '-' && chars[i + 1] == '-' {
            in_comment = true;
            i += 2;
            continue;
        }
        if in_comment && c == '\n' {
            in_comment = false;
            i += 1;
            continue;
        }
        if in_comment {
            i += 1;
            continue;
        }

        // Handle strings
        if !in_string && (c == '\'' || c == '"') {
            in_string = true;
            string_char = c;
            i += 1;
            continue;
        }
        if in_string && c == string_char {
            // Check for escaped quote
            if i + 1 < len && chars[i + 1] == string_char {
                i += 2;
                continue;
            }
            in_string = false;
            i += 1;
            continue;
        }

        // Statement separator
        if !in_string && c == ';' {
            statements.push(&content[start..i]);
            start = i + 1;
        }

        i += 1;
    }

    // Don't forget the last statement
    if start < len {
        let last = content[start..].trim();
        if !last.is_empty() {
            statements.push(&content[start..]);
        }
    }

    statements
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_split_statements_simple() {
        let sql = "SELECT 1; SELECT 2; SELECT 3";
        let stmts = split_statements(sql);
        assert_eq!(stmts.len(), 3);
    }

    #[test]
    fn test_split_statements_with_strings() {
        let sql = "SELECT 'hello; world'; SELECT 2";
        let stmts = split_statements(sql);
        assert_eq!(stmts.len(), 2);
        assert!(stmts[0].contains("'hello; world'"));
    }

    #[test]
    fn test_split_statements_with_comments() {
        let sql = "SELECT 1; -- comment with ; semicolon\nSELECT 2";
        let stmts = split_statements(sql);
        assert_eq!(stmts.len(), 2);
    }

    #[test]
    fn test_split_statements_block_comment() {
        let sql = "SELECT 1; /* block ; comment */ SELECT 2";
        let stmts = split_statements(sql);
        assert_eq!(stmts.len(), 2);
    }
}
