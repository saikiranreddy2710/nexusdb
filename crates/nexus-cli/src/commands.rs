//! Special backslash commands for the REPL.
//!
//! Provides commands like `\d`, `\dt`, `\q`, etc.

use anyhow::Result;

use crate::formatter::OutputFormat;
use crate::repl::Repl;

/// Result of executing a command.
pub enum CommandResult {
    /// Continue the REPL.
    Continue,
    /// Exit the REPL.
    Exit,
    /// Output a message.
    Output(String),
    /// Toggle timing mode.
    ToggleTiming,
    /// Toggle expanded/vertical display mode.
    ToggleExpanded,
    /// Set output format.
    SetFormat(OutputFormat),
    /// Switch to a different database (and optionally user).
    SwitchDatabase { db: String, user: Option<String> },
}

/// A parsed command.
pub enum Command {
    /// Quit the REPL.
    Quit,
    /// Show help.
    Help,
    /// Describe an object.
    Describe(Option<String>),
    /// List tables.
    ListTables,
    /// List databases.
    ListDatabases,
    /// Show connection info.
    ConnectionInfo,
    /// Switch to a different database (and optionally user).
    ConnectDatabase { db: String, user: Option<String> },
    /// Toggle timing.
    Timing,
    /// Toggle expanded/vertical display.
    Expanded,
    /// Set output format.
    Format(String),
    /// Show version.
    Version,
    /// Clear screen.
    Clear,
    /// Execute a file.
    Include(String),
    /// Begin a transaction.
    Begin,
    /// Commit transaction.
    Commit,
    /// Rollback transaction.
    Rollback,
    /// Show server status.
    Status,
    /// Unknown command.
    Unknown(String),
}

impl Command {
    /// Parses a command string.
    pub fn parse(input: &str) -> Self {
        let input = input.trim();
        
        // Remove leading backslash
        let cmd = if input.starts_with('\\') {
            &input[1..]
        } else {
            input
        };

        let parts: Vec<&str> = cmd.splitn(2, char::is_whitespace).collect();
        let cmd_name = parts[0].to_lowercase();
        let args = parts.get(1).map(|s| s.trim().to_string());

        match cmd_name.as_str() {
            "q" | "quit" | "exit" => Command::Quit,
            "?" | "h" | "help" => Command::Help,
            "d" => Command::Describe(args),
            "dt" | "tables" => Command::ListTables,
            "l" | "list" => Command::ListDatabases,
            "conninfo" => Command::ConnectionInfo,
            "c" => match args {
                Some(connect_args) => {
                    // \c dbname [username]  — like psql
                    let parts: Vec<&str> = connect_args.splitn(2, char::is_whitespace).collect();
                    let db = parts[0].trim().to_string();
                    let user = parts.get(1).map(|u| u.trim().to_string()).filter(|u| !u.is_empty());
                    Command::ConnectDatabase { db, user }
                }
                None => Command::ConnectionInfo,
            },
            "timing" | "t" => Command::Timing,
            "x" | "expanded" => Command::Expanded,
            "format" | "f" => Command::Format(args.unwrap_or_else(|| "table".to_string())),
            "version" | "v" => Command::Version,
            "clear" | "cls" => Command::Clear,
            "i" | "include" => Command::Include(args.unwrap_or_default()),
            "begin" => Command::Begin,
            "commit" => Command::Commit,
            "rollback" => Command::Rollback,
            "status" | "s" => Command::Status,
            _ => Command::Unknown(cmd_name),
        }
    }

    /// Executes the command.
    pub async fn execute(&self, repl: &mut Repl) -> Result<CommandResult> {
        match self {
            Command::Quit => Ok(CommandResult::Exit),
            
            Command::Help => Ok(CommandResult::Output(Self::help_text())),
            
            Command::Describe(name) => {
                if let Some(name) = name {
                    self.describe_object(repl, name).await
                } else {
                    self.list_all_objects(repl).await
                }
            }
            
            Command::ListTables => self.list_tables(repl).await,
            
            Command::ListDatabases => self.list_databases(repl).await,
            
            Command::ConnectionInfo => self.connection_info(repl).await,
            
            Command::ConnectDatabase { db, user } => {
                // Validate database exists by executing USE on the server
                if let Some(client) = repl.client() {
                    let sql = format!("USE {}", db);
                    match client.execute(&sql).await {
                        Ok(_) => Ok(CommandResult::SwitchDatabase {
                            db: db.clone(),
                            user: user.clone(),
                        }),
                        Err(e) => Ok(CommandResult::Output(format!(
                            "ERROR: could not switch to database \"{}\": {}",
                            db, e
                        ))),
                    }
                } else {
                    Ok(CommandResult::Output(
                        "Not connected to server.".to_string(),
                    ))
                }
            }
            
            Command::Timing => Ok(CommandResult::ToggleTiming),
            Command::Expanded => Ok(CommandResult::ToggleExpanded),
            
            Command::Format(format) => {
                let fmt = match format.to_lowercase().as_str() {
                    "table" => OutputFormat::Table,
                    "json" => OutputFormat::Json,
                    "csv" => OutputFormat::Csv,
                    "raw" => OutputFormat::Raw,
                    _ => {
                        return Ok(CommandResult::Output(format!(
                            "Unknown format '{}'. Available: table, json, csv, raw",
                            format
                        )));
                    }
                };
                Ok(CommandResult::SetFormat(fmt))
            }
            
            Command::Version => Ok(CommandResult::Output(format!(
                "NexusDB CLI v{}\nNexusDB Client v{}",
                env!("CARGO_PKG_VERSION"),
                env!("CARGO_PKG_VERSION")
            ))),
            
            Command::Clear => {
                // Clear screen using ANSI escape codes
                print!("\x1B[2J\x1B[1;1H");
                Ok(CommandResult::Continue)
            }
            
            Command::Include(path) => {
                if path.is_empty() {
                    Ok(CommandResult::Output("Usage: \\i <filename>".to_string()))
                } else {
                    self.include_file(repl, path).await
                }
            }
            
            Command::Begin => {
                repl.execute_and_print("BEGIN").await?;
                Ok(CommandResult::Continue)
            }
            
            Command::Commit => {
                repl.execute_and_print("COMMIT").await?;
                Ok(CommandResult::Continue)
            }
            
            Command::Rollback => {
                repl.execute_and_print("ROLLBACK").await?;
                Ok(CommandResult::Continue)
            }
            
            Command::Status => self.server_status(repl).await,
            
            Command::Unknown(cmd) => Ok(CommandResult::Output(format!(
                "Unknown command '\\{}'. Type \\? for help.",
                cmd
            ))),
        }
    }

    /// Returns help text.
    fn help_text() -> String {
        r#"NexusDB CLI Commands
======================

Backslash commands (all available):

General:
  \q, \quit       Exit the CLI
  \?, \h, \help   Show this help
  \v, \version    Show version information
  \clear, \cls    Clear screen

Connection:
  \c DBNAME [USER] Switch to database (optionally as user)
  \c, \conninfo    Show connection information
  \s, \status      Show server status (uptime, connections, features)

Schema:
  \d NAME         Describe table (columns, types, nullable, primary key)
  \d              Show hint (use \dt or \d <name>)
  \dt, \tables    List all tables
  \l, \list       List all databases

Transaction:
  \begin          Start a transaction
  \commit         Commit the transaction
  \rollback       Rollback the transaction

Display:
  \x, \expanded   Toggle expanded/vertical display (one column per line)
  \t, \timing     Toggle timing display
  \f FORMAT       Set output format: table, json, csv, raw

Files:
  \i FILE         Execute SQL from file (one statement per line)

SQL supported:
  DDL: CREATE TABLE, DROP TABLE, DESCRIBE
  DML: INSERT (VALUES), UPDATE, DELETE, SELECT
  INSERT (working):
    - VALUES       INSERT INTO t (a,b) VALUES (1,'x'), (2,'y');
    - SELECT       INSERT INTO t SELECT * FROM other;
  INSERT (not yet supported):
    - DEFAULT      INSERT INTO t DEFAULT VALUES;
  Other: SHOW TABLES, SHOW DATABASES, BEGIN, COMMIT, ROLLBACK

Distributed:
  Connect to any node (host:port). Use \s for server status.
  Clustered mode: set enable_raft and raft_peers in server config file.

Type SQL statements followed by a semicolon to execute them.
"#
        .to_string()
    }

    async fn describe_object(&self, repl: &mut Repl, name: &str) -> Result<CommandResult> {
        // Execute DESCRIBE or equivalent
        let sql = format!("DESCRIBE {}", name);
        repl.execute_and_print(&sql).await?;
        Ok(CommandResult::Continue)
    }

    async fn list_all_objects(&self, _repl: &mut Repl) -> Result<CommandResult> {
        // Would list all tables, views, indexes, etc.
        Ok(CommandResult::Output(
            "Use \\dt to list tables, \\d <name> to describe an object.".to_string(),
        ))
    }

    async fn list_tables(&self, repl: &mut Repl) -> Result<CommandResult> {
        // Execute SHOW TABLES command
        repl.execute_and_print("SHOW TABLES").await?;
        Ok(CommandResult::Continue)
    }

    async fn list_databases(&self, repl: &mut Repl) -> Result<CommandResult> {
        // Execute SHOW DATABASES command
        repl.execute_and_print("SHOW DATABASES").await?;
        Ok(CommandResult::Continue)
    }

    async fn connection_info(&self, repl: &mut Repl) -> Result<CommandResult> {
        let config = repl.config();
        let info = format!(
            "Host: {}:{}\nDatabase: {}\nUser: {}",
            config.host,
            config.port,
            config.database.as_deref().unwrap_or("(none)"),
            config.username.as_deref().unwrap_or("(none)")
        );
        Ok(CommandResult::Output(info))
    }

    async fn include_file(&self, repl: &mut Repl, path: &str) -> Result<CommandResult> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| anyhow::anyhow!("Cannot read file '{}': {}", path, e))?;

        // Use proper SQL statement splitting (handles multi-line, comments, strings)
        let statements = crate::split_statements(&content);
        let mut count = 0;

        for stmt in &statements {
            let trimmed = stmt.trim();
            if !trimmed.is_empty() && !trimmed.starts_with("--") {
                repl.execute_and_print(trimmed).await?;
                count += 1;
            }
        }

        Ok(CommandResult::Output(format!(
            "Executed {} statement{} from {}",
            count,
            if count == 1 { "" } else { "s" },
            path
        )))
    }

    async fn server_status(&self, repl: &mut Repl) -> Result<CommandResult> {
        if let Some(client) = repl.client() {
            match client.server_info().await {
                Ok(info) => {
                    let output = format!(
                        "Server: {} v{}\n\
                         Protocol: v{}\n\
                         Uptime: {}s\n\
                         Active connections: {}\n\
                         Features: {}",
                        info.server_name,
                        info.version,
                        info.protocol_version,
                        info.uptime_seconds,
                        info.active_connections,
                        info.features.join(", ")
                    );
                    Ok(CommandResult::Output(output))
                }
                Err(e) => Ok(CommandResult::Output(format!("Failed to get status: {}", e))),
            }
        } else {
            Ok(CommandResult::Output("Not connected to server.".to_string()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_quit() {
        assert!(matches!(Command::parse("\\q"), Command::Quit));
        assert!(matches!(Command::parse("\\quit"), Command::Quit));
        assert!(matches!(Command::parse("\\exit"), Command::Quit));
    }

    #[test]
    fn test_parse_help() {
        assert!(matches!(Command::parse("\\?"), Command::Help));
        assert!(matches!(Command::parse("\\h"), Command::Help));
        assert!(matches!(Command::parse("\\help"), Command::Help));
    }

    #[test]
    fn test_parse_describe() {
        match Command::parse("\\d users") {
            Command::Describe(Some(name)) => assert_eq!(name, "users"),
            _ => panic!("Expected Describe"),
        }
    }

    #[test]
    fn test_parse_format() {
        match Command::parse("\\f json") {
            Command::Format(fmt) => assert_eq!(fmt, "json"),
            _ => panic!("Expected Format"),
        }
    }

    #[test]
    fn test_parse_unknown() {
        match Command::parse("\\xyz") {
            Command::Unknown(cmd) => assert_eq!(cmd, "xyz"),
            _ => panic!("Expected Unknown"),
        }
    }
}
