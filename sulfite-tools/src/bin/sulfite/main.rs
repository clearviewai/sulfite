mod by_csv;
mod list;
mod op;

use anyhow::Result;
use clap::{Parser, Subcommand};

/// S3 object operations: list, single-object op, or batch by CSV
#[derive(Parser)]
#[command(name = "sulfite")]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    /// The region
    #[arg(short, long, global = true)]
    region: Option<String>,
    /// The endpoint URL
    #[arg(short, long, global = true)]
    endpoint_url: Option<String>,
}

#[derive(Subcommand)]
enum Commands {
    /// List objects in a bucket with optional prefix/suffix and output to CSV
    List(ListArgs),
    /// Single-object operation: head, download, upload, delete, copy, restore
    Op(OpArgs),
    /// Batch operations from a CSV of keys: head, download, upload, delete, copy, restore
    ByCsv(ByCsvArgs),
}

#[derive(Parser)]
struct ListArgs {
    /// The bucket name
    #[arg(short, long)]
    bucket: String,
    /// The prefix in a bucket
    #[arg(short, long)]
    prefix: String,
    /// The suffix name for records storage
    #[arg(short, long, default_value = "")]
    suffix: String,
    /// The delimiter, default to '/' for ease and consistency with aws-cli
    #[arg(short, long, default_value = "/")]
    delimiter: Option<String>,
    /// head
    #[arg(long, default_value = "10")]
    head: usize,
    /// Output dir
    #[arg(short, long)]
    output_path: Option<String>,
    /// keep the prefix from the key in the output file - by default it is removed
    #[arg(long, default_value = "false")]
    keep_prefix: bool,
    /// remove the suffix from the key in the output file - by default it is kept
    #[arg(long, default_value = "false")]
    remove_suffix: bool,
}

#[derive(Parser)]
struct OpArgs {
    /// Operation: head, download, download-multipart, upload, upload-multipart, delete, copy, restore
    op: String,
    /// The bucket name
    #[arg(short, long)]
    bucket: String,
    /// The key in a bucket
    #[arg(short, short_alias = 'p', long)]
    key: String,
    /// local_path
    #[arg(short, long)]
    local_path: Option<String>,
    /// start offset
    #[arg(long)]
    start_offset: Option<usize>,
    /// end offset
    #[arg(long)]
    end_offset: Option<usize>,
    /// src_bucket: source bucket for copy operation
    #[arg(long)]
    src_bucket: Option<String>,
    /// src_key: source key for copy operation
    #[arg(long)]
    src_key: Option<String>,
    /// storage_class: STANDARD, GLACIER, DEEP_ARCHIVE, ...
    #[arg(long)]
    storage_class: Option<String>,
    /// restore tier: Bulk, Standard, (and Expedited for Glacier)
    #[arg(long, default_value = "Standard")]
    restore_tier: String,
    /// restore days
    #[arg(long, default_value = "1")]
    restore_days: i32,
    /// n_workers
    #[arg(short, long, default_value = "250")]
    n_workers: usize,
}

#[derive(Parser)]
struct ByCsvArgs {
    /// Operation: head, download, download-flatten (flattens filepaths), upload, delete, copy, restore
    op: String,
    /// Input csv path
    source_path: String,
    /// column index
    #[arg(short, long, default_value = "0")]
    column_idx: usize,
    /// has header
    #[arg(long, default_value = "false")]
    has_header: bool,
    /// local dir for downloads and uploads
    #[arg(short, long)]
    local_dir: Option<String>,
    /// The bucket name
    #[arg(short, long)]
    bucket: String,
    /// The prefix before provided keys
    #[arg(short, long)]
    prefix: String,
    /// The suffix after provided keys
    #[arg(short, long, default_value = "")]
    suffix: String,
    /// src_bucket: source bucket for copy operation
    #[arg(long)]
    src_bucket: Option<String>,
    /// The source prefix before provided keys
    #[arg(long, default_value = "")]
    src_prefix: String,
    /// The source suffix after provided keys
    #[arg(long, default_value = "")]
    src_suffix: String,
    /// storage_class: STANDARD, GLACIER, DEEP_ARCHIVE, ...
    #[arg(long)]
    storage_class: Option<String>,
    /// restore tier: Bulk, Standard, (and Expedited for Glacier)
    #[arg(long, default_value = "Standard")]
    restore_tier: String,
    /// restore days
    #[arg(long, default_value = "1")]
    restore_days: i32,
    /// n workers
    #[arg(short, long, default_value = "250")]
    n_workers: usize,
    /// n inner workers
    #[arg(long, default_value = "10")]
    n_inner_workers: usize,
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    let args = Cli::parse();

    match args.command {
        Commands::List(list_args) => {
            list::run_list(&args.region, &args.endpoint_url, list_args).await
        }
        Commands::Op(op_args) => op::run_op(&args.region, &args.endpoint_url, op_args).await,
        Commands::ByCsv(by_csv_args) => {
            by_csv::run_by_csv(&args.region, &args.endpoint_url, by_csv_args).await
        }
    }
}
