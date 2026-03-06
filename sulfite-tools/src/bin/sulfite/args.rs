//! Clap CLI definitions for sulfite.

use clap::{Parser, Subcommand};
use sulfite::{
    DEFAULT_MULTIPART_CHUNK_SIZE, DEFAULT_READ_TIMEOUT, DEFAULT_RETRIABLE_CLIENT_STATUS_CODES_STR,
};

/// S3 operations: list, single-object ops, or batch from CSV.
#[derive(Parser)]
#[command(name = "sulfite")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,

    /// The AWS region (or the region of the custom endpoint).
    #[arg(short, long, global = true)]
    pub region: Option<String>,
    /// The S3 endpoint URL (e.g. for MinIO or a custom endpoint).
    #[arg(short, long, global = true)]
    pub endpoint_url: Option<String>,
    /// Maximum number of retries per request.
    #[arg(long, global = true, default_value = "3")]
    pub max_retries: usize,
    /// HTTP status codes to treat as retriable client errors (comma-separated).
    #[arg(long, global = true, value_delimiter(','), value_parser = clap::value_parser!(u16), num_args(1..), default_value = DEFAULT_RETRIABLE_CLIENT_STATUS_CODES_STR)]
    pub retriable_client_status_codes: Vec<u16>,
    /// Read timeout in seconds for the HTTP client.
    #[arg(long, global = true, default_value_t = DEFAULT_READ_TIMEOUT)]
    pub read_timeout: u64,
    /// Part size in bytes for multipart upload/download (default: 20 MiB).
    #[arg(long, global = true, default_value_t = DEFAULT_MULTIPART_CHUNK_SIZE)]
    pub multipart_chunk_size: u64,
    /// Number of parallel workers for multipart upload/download.
    #[arg(long, global = true, default_value_t = 10)]
    pub multipart_workers: usize,
}

#[derive(Subcommand)]
pub enum Command {
    /// List objects in a bucket (optional prefix/suffix) and write keys to CSV.
    List(ListArgs),
    /// Get metadata (HEAD) for one object.
    Head(HeadArgs),
    /// Download one object (single request).
    Download(DownloadArgs),
    /// Download one object (multipart transfer).
    DownloadMultipart(DownloadMultipartArgs),
    /// Upload one object (single request).
    Upload(UploadArgs),
    /// Upload one object (multipart transfer).
    UploadMultipart(UploadMultipartArgs),
    /// Delete one object.
    Delete(DeleteArgs),
    /// Copy one object from source to destination.
    Copy(CopyArgs),
    /// Restore one object from archival storage (e.g. Glacier).
    Restore(RestoreArgs),
    /// Run one operation per key from a CSV file (batch).
    Csv(CsvArgs),
}

#[derive(Parser, Clone)]
pub struct ListArgs {
    /// The name of the S3 bucket.
    #[arg(short, long)]
    pub bucket: String,
    /// The prefix to list under.
    #[arg(short, long)]
    pub prefix: String,
    /// Only list keys that end with this suffix.
    #[arg(short, long, default_value = "")]
    pub suffix: String,
    /// The delimiter for common-prefix grouping (e.g. '/').
    #[arg(short, long, default_value = "/")]
    pub delimiter: String,
    /// Maximum number of object keys and common prefixes to show in the console preview. Does not limit CSV output or listing.
    #[arg(short = 'm', long, default_value = "10")]
    pub display_max_entries: usize,
    /// The path to write the key list as CSV (omit to print to stdout).
    #[arg(short, long)]
    pub output_path: Option<String>,
    /// Keep the prefix in the output keys (by default the prefix is stripped).
    #[arg(long, default_value = "false")]
    pub keep_prefix: bool,
    /// Strip the suffix from the output keys (by default the suffix is kept).
    #[arg(long, default_value = "false")]
    pub remove_suffix: bool,
}

#[derive(Parser, Clone)]
pub struct HeadArgs {
    /// The name of the S3 bucket.
    #[arg(short, long)]
    pub bucket: String,
    /// The object key.
    #[arg(short, long)]
    pub key: String,
}

#[derive(Parser, Clone)]
pub struct DownloadArgs {
    /// The name of the S3 bucket.
    #[arg(short, long)]
    pub bucket: String,
    /// The object key.
    #[arg(short, long)]
    pub key: String,
    /// The local path to write to (defaults to current directory with filename from key).
    #[arg(short, long)]
    pub local_path: Option<String>,
    /// The start offset in bytes for a range download (optional).
    #[arg(long)]
    pub start_offset: Option<usize>,
    /// The end offset in bytes for a range download (optional).
    #[arg(long)]
    pub end_offset: Option<usize>,
}

#[derive(Parser, Clone)]
pub struct DownloadMultipartArgs {
    /// The name of the S3 bucket.
    #[arg(short, long)]
    pub bucket: String,
    /// The object key.
    #[arg(short, long)]
    pub key: String,
    /// The local path to write to (defaults to current directory with filename from key).
    #[arg(short, long)]
    pub local_path: Option<String>,
    /// The number of parallel workers for part downloads.
    #[arg(short, long, default_value = "250")]
    pub n_workers: usize,
}

#[derive(Parser, Clone)]
pub struct UploadArgs {
    /// The name of the S3 bucket.
    #[arg(short, long)]
    pub bucket: String,
    /// The object key.
    #[arg(short, long)]
    pub key: String,
    /// The local file path to upload.
    #[arg(short, long)]
    pub local_path: String,
    /// The storage class (e.g. STANDARD, GLACIER).
    #[arg(long)]
    pub storage_class: Option<String>,
}

#[derive(Parser, Clone)]
pub struct UploadMultipartArgs {
    /// The name of the S3 bucket.
    #[arg(short, long)]
    pub bucket: String,
    /// The object key.
    #[arg(short, long)]
    pub key: String,
    /// The local file path to upload.
    #[arg(short, long)]
    pub local_path: String,
    /// The storage class (e.g. STANDARD, GLACIER).
    #[arg(long)]
    pub storage_class: Option<String>,
    /// The number of parallel workers for part uploads.
    #[arg(short, long, default_value = "250")]
    pub n_workers: usize,
}

#[derive(Parser, Clone)]
pub struct DeleteArgs {
    /// The name of the S3 bucket.
    #[arg(short, long)]
    pub bucket: String,
    /// The object key.
    #[arg(short, long)]
    pub key: String,
}

#[derive(Parser, Clone)]
pub struct CopyArgs {
    /// The source bucket name.
    #[arg(long)]
    pub src_bucket: String,
    /// The source object key.
    #[arg(long)]
    pub src_key: String,
    /// The destination bucket name.
    #[arg(long)]
    pub dst_bucket: String,
    /// The destination object key.
    #[arg(long)]
    pub dst_key: String,
    /// The storage class for the copied object (e.g. STANDARD, GLACIER).
    #[arg(long)]
    pub storage_class: Option<String>,
}

#[derive(Parser, Clone)]
pub struct RestoreArgs {
    /// The name of the S3 bucket.
    #[arg(short, long)]
    pub bucket: String,
    /// The object key.
    #[arg(short, long)]
    pub key: String,
    /// The restore tier (e.g. Standard, Bulk, Expedited).
    #[arg(long, default_value = "Standard")]
    pub restore_tier: String,
    /// The number of days to keep the restored copy available.
    #[arg(long, default_value = "1")]
    pub restore_days: i32,
}

/// Internal enum for dispatching single-object operations to `obj::run_obj`.
#[derive(Clone)]
pub enum ObjCommand {
    Head(HeadArgs),
    Download(DownloadArgs),
    DownloadMultipart(DownloadMultipartArgs),
    Upload(UploadArgs),
    UploadMultipart(UploadMultipartArgs),
    Delete(DeleteArgs),
    Copy(CopyArgs),
    Restore(RestoreArgs),
}

/// Arguments for CSV-driven batch operations.
#[derive(Parser, Clone)]
pub struct CsvArgs {
    #[command(subcommand)]
    pub command: CsvCommand,

    /// The path to the CSV file.
    pub source_path: String,
    /// The zero-based column index in the CSV that holds the object key.
    #[arg(short, long, default_value = "0")]
    pub column_idx: usize,
    /// Whether the CSV has a header row (the first row is skipped when reading keys).
    #[arg(long, default_value = "false")]
    pub has_header: bool,
    /// Maximum number of keys to process in parallel (batch-level parallelism).
    #[arg(short, long, default_value = "250")]
    pub n_workers: usize,
}

#[derive(Subcommand, Clone)]
pub enum CsvCommand {
    /// Get metadata (HEAD) for each key and print it.
    Head {
        /// The name of the S3 bucket.
        #[arg(short, long)]
        bucket: String,
        /// The prefix prepended to each key from the CSV.
        #[arg(short, long)]
        prefix: String,
        /// The suffix appended to each key from the CSV.
        #[arg(short, long, default_value = "")]
        suffix: String,
    },
    /// Download each key; preserve key path under the local directory.
    Download {
        /// The name of the S3 bucket.
        #[arg(short, long)]
        bucket: String,
        /// The prefix prepended to each key from the CSV.
        #[arg(short, long)]
        prefix: String,
        /// The suffix appended to each key from the CSV.
        #[arg(short, long, default_value = "")]
        suffix: String,
        /// The local directory to download into (defaults to current directory).
        #[arg(short, long, default_value = ".")]
        local_dir: String,
    },
    /// Upload each key from files under the local directory.
    Upload {
        /// The name of the S3 bucket.
        #[arg(short, long)]
        bucket: String,
        /// The prefix prepended to each key from the CSV.
        #[arg(short, long)]
        prefix: String,
        /// The suffix appended to each key from the CSV.
        #[arg(short, long, default_value = "")]
        suffix: String,
        /// The local directory to read files from (defaults to current directory).
        #[arg(short, long, default_value = ".")]
        local_dir: String,
        /// The storage class (e.g. STANDARD, GLACIER).
        #[arg(long)]
        storage_class: Option<String>,
    },
    /// Delete each key.
    Delete {
        /// The name of the S3 bucket.
        #[arg(short, long)]
        bucket: String,
        /// The prefix prepended to each key from the CSV.
        #[arg(short, long)]
        prefix: String,
        /// The suffix appended to each key from the CSV.
        #[arg(short, long, default_value = "")]
        suffix: String,
    },
    /// Copy each key from source bucket to destination.
    Copy {
        /// The source bucket name.
        #[arg(long)]
        src_bucket: String,
        /// The prefix prepended to each key for the source.
        #[arg(long)]
        src_prefix: String,
        /// The suffix appended to each key for the source.
        #[arg(long, default_value = "")]
        src_suffix: String,
        /// The destination bucket name.
        #[arg(long)]
        dst_bucket: String,
        /// The prefix prepended to each key for the destination.
        #[arg(long)]
        dst_prefix: String,
        /// The suffix appended to each key for the destination.
        #[arg(long, default_value = "")]
        dst_suffix: String,
        /// The storage class for the copy (e.g. STANDARD, GLACIER).
        #[arg(long)]
        storage_class: Option<String>,
    },
    /// Restore each key from archival storage (e.g. Glacier).
    Restore {
        /// The name of the S3 bucket.
        #[arg(short, long)]
        bucket: String,
        /// The prefix prepended to each key from the CSV.
        #[arg(short, long)]
        prefix: String,
        /// The suffix appended to each key from the CSV.
        #[arg(short, long, default_value = "")]
        suffix: String,
        /// The restore tier (e.g. Standard, Bulk, Expedited).
        #[arg(long, default_value = "Standard")]
        restore_tier: String,
        /// The number of days to keep the restored copy available.
        #[arg(long, default_value = "1")]
        restore_days: i32,
    },
}
