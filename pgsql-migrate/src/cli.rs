use clap::{Parser, Subcommand, ValueEnum};

#[derive(Debug, Clone, ValueEnum)]
pub enum SafeModeConfirm {
    #[value(name = "ask")]
    Ask,
    #[value(name = "exit-with-error")]
    ExitWithError,
}

#[derive(Parser)]
#[command(name = "pgsql-migrate")]
#[command(about = "A simple PostgreSQL migration tool", long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    #[command(name = "up")]
    Up {
        #[arg(short = 'p', long = "path")]
        path: Option<String>,

        #[arg(short = 'd', long = "database")]
        database: Option<String>,

        #[arg(short = 'e', long = "env")]
        env: Option<String>,

        #[arg(
            long = "safe-mode",
            value_name = "TABLES",
            help = "Comma-separated table names to watch for in pending migrations"
        )]
        safe_mode: Option<String>,

        #[arg(
            long = "safe-mode-confirm",
            value_enum,
            default_value = "ask",
            help = "Action when unacknowledged safe-mode table found: ask (default) or exit-with-error"
        )]
        safe_mode_confirm: SafeModeConfirm,
    },

    #[command(name = "down")]
    Down {
        #[arg(short = 'p', long = "path")]
        path: Option<String>,

        #[arg(short = 'd', long = "database")]
        database: Option<String>,

        #[arg(short = 'e', long = "env")]
        env: Option<String>,

        #[arg(default_value = "1")]
        count: u32,

        #[arg(
            long = "safe-mode-skip-auto-remove",
            help = "Skip automatic removal of safe-mode.yml entries when rolling back"
        )]
        safe_mode_skip_auto_remove: bool,
    },

    #[command(name = "create")]
    Create {
        #[arg(short = 'd', long = "dir", default_value = "migrations")]
        dir: String,

        #[arg(short = 's', long = "seq")]
        name: String,
    },

    #[command(name = "baseline")]
    Baseline {
        #[arg(short = 'p', long = "path", default_value = "migrations")]
        path: String,

        #[arg(short = 'd', long = "database")]
        database: String,

        #[arg(short = 'v', long = "version")]
        version: u32,
    },

    #[command(name = "redo")]
    Redo {
        #[arg(short = 'p', long = "path", default_value = "migrations")]
        path: String,

        #[arg(short = 'd', long = "database")]
        database: String,

        #[arg(short = 'e', long = "env", default_value = "prod")]
        env: String,

        #[arg(
            long = "safe-mode",
            value_name = "TABLES",
            help = "Comma-separated table names to watch for in pending migrations"
        )]
        safe_mode: Option<String>,

        #[arg(
            long = "safe-mode-confirm",
            value_enum,
            default_value = "ask",
            help = "Action when unacknowledged safe-mode table found: ask (default) or exit-with-error"
        )]
        safe_mode_confirm: SafeModeConfirm,
    },
    #[command(name = "backup")]
    Backup {
        #[arg(short = 'd', long = "database")]
        database: String,

        #[arg(short = 'o', long = "output")]
        output: String,

        #[arg(short = 'f', long = "format", default_value = "custom")]
        format: String,

        #[arg(short = 'c', long = "compress")]
        compress: Option<u8>,

        #[arg(short = 'j', long = "jobs")]
        jobs: Option<u8>,

        #[arg(long = "no-owner")]
        no_owner: bool,

        #[arg(long = "no-acl")]
        no_acl: bool,

        #[arg(long = "max-retain-days", default_value = "15")]
        max_retain_days: Option<u64>,
    },

    #[command(name = "restore")]
    Restore {
        #[arg(short = 'd', long = "database")]
        database: String,

        #[arg(short = 'i', long = "input")]
        input: String,

        #[arg(long = "clean")]
        clean: bool,

        #[arg(long = "create")]
        create: bool,

        #[arg(long = "no-owner")]
        no_owner: bool,

        #[arg(long = "no-acl")]
        no_acl: bool,
    },
}
