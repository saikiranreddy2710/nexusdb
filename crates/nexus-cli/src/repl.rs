//! Interactive REPL (Read-Eval-Print-Loop) for NexusDB.
//!
//! Provides an interactive SQL shell with command history, line editing,
//! and multi-line input support.

use std::borrow::Cow;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use rustyline::completion::{Completer, Pair};
use rustyline::error::ReadlineError;
use rustyline::highlight::Highlighter;
use rustyline::hint::Hinter;
use rustyline::history::DefaultHistory;
use rustyline::validate::{ValidationContext, ValidationResult, Validator};
use rustyline::{CompletionType, Config, EditMode, Editor, Helper};
use tracing::{debug, error};

use nexus_client::{Client, ClientConfig, QueryResult};

use crate::commands::{Command, CommandResult};
use crate::config::CliConfig;
use crate::formatter::{self, OutputFormat};

/// The REPL prompt shown when waiting for input.
const PROMPT: &str = "nexus> ";

/// The continuation prompt for multi-line input.
const CONTINUATION_PROMPT: &str = "    -> ";

/// REPL helper for rustyline.
struct ReplHelper {
    /// SQL keywords for completion.
    keywords: Vec<String>,
}

impl ReplHelper {
    fn new() -> Self {
        Self {
            keywords: vec![
                "SELECT".to_string(),
                "FROM".to_string(),
                "WHERE".to_string(),
                "INSERT".to_string(),
                "INTO".to_string(),
                "VALUES".to_string(),
                "UPDATE".to_string(),
                "SET".to_string(),
                "DELETE".to_string(),
                "CREATE".to_string(),
                "TABLE".to_string(),
                "DROP".to_string(),
                "ALTER".to_string(),
                "INDEX".to_string(),
                "PRIMARY".to_string(),
                "KEY".to_string(),
                "FOREIGN".to_string(),
                "REFERENCES".to_string(),
                "JOIN".to_string(),
                "LEFT".to_string(),
                "RIGHT".to_string(),
                "INNER".to_string(),
                "OUTER".to_string(),
                "ON".to_string(),
                "AND".to_string(),
                "OR".to_string(),
                "NOT".to_string(),
                "NULL".to_string(),
                "IS".to_string(),
                "IN".to_string(),
                "LIKE".to_string(),
                "BETWEEN".to_string(),
                "ORDER".to_string(),
                "BY".to_string(),
                "ASC".to_string(),
                "DESC".to_string(),
                "LIMIT".to_string(),
                "OFFSET".to_string(),
                "GROUP".to_string(),
                "HAVING".to_string(),
                "DISTINCT".to_string(),
                "AS".to_string(),
                "CASE".to_string(),
                "WHEN".to_string(),
                "THEN".to_string(),
                "ELSE".to_string(),
                "END".to_string(),
                "COUNT".to_string(),
                "SUM".to_string(),
                "AVG".to_string(),
                "MIN".to_string(),
                "MAX".to_string(),
                "BEGIN".to_string(),
                "COMMIT".to_string(),
                "ROLLBACK".to_string(),
                "TRANSACTION".to_string(),
                "EXPLAIN".to_string(),
                "ANALYZE".to_string(),
                "INTEGER".to_string(),
                "INT".to_string(),
                "BIGINT".to_string(),
                "SMALLINT".to_string(),
                "FLOAT".to_string(),
                "DOUBLE".to_string(),
                "BOOLEAN".to_string(),
                "VARCHAR".to_string(),
                "TEXT".to_string(),
                "TIMESTAMP".to_string(),
                "DATE".to_string(),
                "TIME".to_string(),
                "SERIAL".to_string(),
            ],
        }
    }
}

impl Completer for ReplHelper {
    type Candidate = Pair;

    fn complete(
        &self,
        line: &str,
        pos: usize,
        _ctx: &rustyline::Context<'_>,
    ) -> rustyline::Result<(usize, Vec<Pair>)> {
        // Find the word being typed
        let start = line[..pos]
            .rfind(|c: char| c.is_whitespace() || c == '(' || c == ',')
            .map(|i| i + 1)
            .unwrap_or(0);

        let word = &line[start..pos];
        let word_upper = word.to_uppercase();

        // Filter keywords that match
        let matches: Vec<Pair> = self
            .keywords
            .iter()
            .filter(|kw| kw.starts_with(&word_upper))
            .map(|kw| Pair {
                display: kw.clone(),
                replacement: kw.clone(),
            })
            .collect();

        Ok((start, matches))
    }
}

impl Hinter for ReplHelper {
    type Hint = String;

    fn hint(&self, _line: &str, _pos: usize, _ctx: &rustyline::Context<'_>) -> Option<Self::Hint> {
        None
    }
}

impl Highlighter for ReplHelper {
    fn highlight<'l>(&self, line: &'l str, _pos: usize) -> Cow<'l, str> {
        // For now, just return the line as-is
        // Could add syntax highlighting here
        Cow::Borrowed(line)
    }

    fn highlight_prompt<'b, 's: 'b, 'p: 'b>(
        &'s self,
        prompt: &'p str,
        _default: bool,
    ) -> Cow<'b, str> {
        Cow::Borrowed(prompt)
    }

    fn highlight_hint<'h>(&self, hint: &'h str) -> Cow<'h, str> {
        Cow::Borrowed(hint)
    }

    fn highlight_char(&self, _line: &str, _pos: usize, _forced: bool) -> bool {
        false
    }
}

impl Validator for ReplHelper {
    fn validate(&self, ctx: &mut ValidationContext) -> rustyline::Result<ValidationResult> {
        let input = ctx.input();
        let trimmed = input.trim();

        // Empty input is valid
        if trimmed.is_empty() {
            return Ok(ValidationResult::Valid(None));
        }

        // Commands starting with \ are always complete
        if trimmed.starts_with('\\') {
            return Ok(ValidationResult::Valid(None));
        }

        // Check if statement appears complete (ends with semicolon)
        if trimmed.ends_with(';') {
            return Ok(ValidationResult::Valid(None));
        }

        // Otherwise, request more input
        Ok(ValidationResult::Incomplete)
    }
}

impl Helper for ReplHelper {}

/// Interactive REPL for NexusDB.
pub struct Repl {
    /// CLI configuration.
    config: CliConfig,
    /// The database client.
    client: Option<Client>,
    /// The rustyline editor.
    editor: Editor<ReplHelper, DefaultHistory>,
    /// Output format.
    format: OutputFormat,
    /// History file path.
    history_file: Option<std::path::PathBuf>,
    /// Whether we're in a transaction.
    in_transaction: bool,
    /// Timing mode enabled.
    timing: bool,
}

impl Repl {
    /// Creates a new REPL instance.
    pub fn new(config: CliConfig, format: OutputFormat) -> Result<Self> {
        let rl_config = Config::builder()
            .history_ignore_space(true)
            .completion_type(CompletionType::List)
            .edit_mode(EditMode::Emacs)
            .max_history_size(1000)?
            .build();

        let mut editor = Editor::with_config(rl_config)?;
        editor.set_helper(Some(ReplHelper::new()));

        // Try to load history
        let history_file = get_history_file();
        if let Some(ref path) = history_file {
            if path.exists() {
                let _ = editor.load_history(path);
            }
        }

        Ok(Self {
            config,
            client: None,
            editor,
            format,
            history_file,
            in_transaction: false,
            timing: false,
        })
    }

    /// Prints the welcome banner.
    pub fn print_banner(&self) {
        println!("NexusDB CLI v{}", env!("CARGO_PKG_VERSION"));
        println!("Type \\? for help, \\q to quit.\n");
    }

    /// Connects to the database.
    pub async fn connect(&mut self) -> Result<()> {
        if self.config.mock_mode {
            // Use mock client for testing
            let client = Client::connect_default()?;
            self.client = Some(client);
            println!("Connected to NexusDB (mock mode)");
            return Ok(());
        }

        let client_config = ClientConfig::new()
            .host(&self.config.host)
            .port(self.config.port)
            .application_name("nexus-cli");

        let client_config = if let Some(ref db) = self.config.database {
            client_config.database(db)
        } else {
            client_config
        };

        let client_config = if let Some(ref user) = self.config.username {
            client_config.username(user)
        } else {
            client_config
        };

        let client_config = if let Some(ref pass) = self.config.password {
            client_config.password(pass)
        } else {
            client_config
        };

        let client = Client::new(client_config);

        match client.connect().await {
            Ok(()) => {
                let db_name = self.config.database.as_deref().unwrap_or("nexusdb");
                println!(
                    "Connected to {}:{} (database: {})",
                    self.config.host, self.config.port, db_name
                );

                // Try to get server info
                if let Ok(info) = client.server_info().await {
                    println!("Server: {} v{}", info.server_name, info.version);
                }

                self.client = Some(client);
                Ok(())
            }
            Err(e) => {
                // Fall back to mock mode if connection fails
                eprintln!("Warning: Could not connect to server: {}", e);
                eprintln!("Starting in mock mode...\n");

                let mock_client = Client::connect_default()?;
                self.client = Some(mock_client);
                Ok(())
            }
        }
    }

    /// Runs the main REPL loop.
    pub async fn run(&mut self) -> Result<()> {
        loop {
            let prompt = self.get_prompt();

            match self.editor.readline(&prompt) {
                Ok(line) => {
                    let line = line.trim();

                    if line.is_empty() {
                        continue;
                    }

                    // Add to history
                    let _ = self.editor.add_history_entry(line);

                    // Process the line
                    match self.process_line(line).await {
                        Ok(should_exit) => {
                            if should_exit {
                                break;
                            }
                        }
                        Err(e) => {
                            eprintln!("Error: {}", e);
                        }
                    }
                }
                Err(ReadlineError::Interrupted) => {
                    println!("^C");
                    // Cancel current input but don't exit
                    continue;
                }
                Err(ReadlineError::Eof) => {
                    println!("\\q");
                    break;
                }
                Err(e) => {
                    error!("Readline error: {}", e);
                    break;
                }
            }
        }

        // Save history
        self.save_history();

        // Disconnect
        if let Some(ref client) = self.client {
            let _ = client.disconnect().await;
        }

        println!("Goodbye!");
        Ok(())
    }

    /// Gets the current prompt.
    fn get_prompt(&self) -> String {
        if self.in_transaction {
            "nexus*> ".to_string()
        } else {
            PROMPT.to_string()
        }
    }

    /// Processes a single line of input.
    async fn process_line(&mut self, line: &str) -> Result<bool> {
        // Check for special commands
        if line.starts_with('\\') {
            return self.process_command(line).await;
        }

        // Execute SQL
        self.execute_and_print(line).await?;
        Ok(false)
    }

    /// Processes a backslash command.
    async fn process_command(&mut self, line: &str) -> Result<bool> {
        let cmd = Command::parse(line);

        match cmd.execute(self).await? {
            CommandResult::Continue => Ok(false),
            CommandResult::Exit => Ok(true),
            CommandResult::Output(msg) => {
                println!("{}", msg);
                Ok(false)
            }
            CommandResult::ToggleTiming(enabled) => {
                self.timing = enabled;
                if enabled {
                    println!("Timing is on.");
                } else {
                    println!("Timing is off.");
                }
                Ok(false)
            }
            CommandResult::SetFormat(format) => {
                self.format = format;
                println!("Output format set to {:?}.", format);
                Ok(false)
            }
        }
    }

    /// Executes SQL and prints the result.
    pub async fn execute_and_print(&mut self, sql: &str) -> Result<()> {
        let client = self.client.as_ref().context("Not connected to database")?;

        let start = Instant::now();

        // Check for transaction control statements
        let sql_upper = sql.to_uppercase();
        if sql_upper.starts_with("BEGIN") {
            self.in_transaction = true;
        } else if sql_upper.starts_with("COMMIT") || sql_upper.starts_with("ROLLBACK") {
            self.in_transaction = false;
        }

        match client.execute(sql).await {
            Ok(result) => {
                let elapsed = start.elapsed();
                self.print_result(&result, elapsed);
                Ok(())
            }
            Err(e) => {
                eprintln!("ERROR: {}", e);
                Ok(())
            }
        }
    }

    /// Prints a query result.
    fn print_result(&self, result: &QueryResult, elapsed: Duration) {
        if result.has_rows() {
            // Format and print the result set
            let output = formatter::format_result(result, self.format);
            println!("{}", output);
            println!(
                "({} row{})",
                result.row_count(),
                if result.row_count() == 1 { "" } else { "s" }
            );
        } else if result.rows_affected > 0 {
            // DML result
            println!(
                "{} row{} affected",
                result.rows_affected,
                if result.rows_affected == 1 { "" } else { "s" }
            );
        } else {
            // DDL or empty result
            println!("OK");
        }

        if self.timing {
            println!("Time: {:.3}ms", elapsed.as_secs_f64() * 1000.0);
        }
    }

    /// Saves command history.
    fn save_history(&mut self) {
        if let Some(ref path) = self.history_file {
            // Ensure parent directory exists
            if let Some(parent) = path.parent() {
                let _ = std::fs::create_dir_all(parent);
            }
            if let Err(e) = self.editor.save_history(path) {
                debug!("Failed to save history: {}", e);
            }
        }
    }

    /// Returns a reference to the client configuration.
    pub fn config(&self) -> &CliConfig {
        &self.config
    }

    /// Returns the current output format.
    pub fn format(&self) -> OutputFormat {
        self.format
    }

    /// Gets the database client (if connected).
    pub fn client(&self) -> Option<&Client> {
        self.client.as_ref()
    }
}

/// Gets the history file path.
fn get_history_file() -> Option<std::path::PathBuf> {
    dirs::data_local_dir().map(|dir| dir.join("nexusdb").join("history"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_history_file() {
        let path = get_history_file();
        assert!(path.is_some());
        let path = path.unwrap();
        assert!(path.ends_with("history"));
    }
}
