use anyhow::{bail, Context, Result};
use clap::Parser;
use sulfite::S3Client;
use sulfite_tools::utils::make_progress_bar;

#[derive(Parser)]
struct Cli {
    /// Operation: head, download, download-multipart, upload, upload-multipart, delete, copy, restore
    op: String,
    /// The bucket name
    #[clap(short, long)]
    bucket: String,
    /// The key in a bucket
    #[clap(short, short_alias = 'p', long)]
    key: String,
    /// local_path
    #[clap(short, long)]
    local_path: Option<String>,
    /// start offset
    #[clap(long)]
    start_offset: Option<usize>,
    /// end offset
    #[clap(long)]
    end_offset: Option<usize>,
    /// src_bucket: source bucket for copy operation
    #[clap(long)]
    src_bucket: Option<String>,
    /// src_key: source key for copy operation
    #[clap(long)]
    src_key: Option<String>,
    /// storage_class: STANDARD, GLACIER, DEEP_ARCHIVE, ...
    #[clap(long)]
    storage_class: Option<String>,
    /// restore tier: Bulk, Standard, (and Expedited for Glacier)
    #[clap(long, default_value = "Standard")]
    restore_tier: String,
    /// restore days
    #[clap(long, default_value = "1")]
    restore_days: i32,
    /// The region
    #[clap(short, long)]
    region: Option<String>,
    /// The endpoint URL
    #[clap(short, long)]
    endpoint_url: Option<String>,
    /// n_workers
    #[clap(short, long, default_value = "250")]
    n_workers: usize,
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    let args = Cli::parse();

    let client = S3Client::new(
        args.region,
        args.endpoint_url,
        None,
        None,
        None,
        None,
        Some(10),
    )
    .await;

    match args.op.as_str() {
        "head" => {
            let object_info = client.head_object(&args.bucket, &args.key).await?;
            println!("{:?}", object_info);
        }
        "download" => {
            let local_path = match &args.local_path {
                Some(local_path) => local_path.as_str(),
                None => std::path::Path::new(&args.key)
                    .file_name()
                    .and_then(|os_str| os_str.to_str())
                    .context("can't convert")?,
            };
            let start_end_offsets = args
                .start_offset
                .zip(args.end_offset)
                .map(|(start, end)| (start, end));
            client
                .download_object(&args.bucket, &args.key, local_path, start_end_offsets)
                .await?;
        }
        "download-multipart" => {
            let local_path = match &args.local_path {
                Some(local_path) => local_path.as_str(),
                None => std::path::Path::new(&args.key)
                    .file_name()
                    .and_then(|os_str| os_str.to_str())
                    .context("can't convert")?,
            };
            let pb = make_progress_bar(Some(0));
            client
                .download_object_multipart(
                    &args.bucket,
                    &args.key,
                    local_path,
                    Some(args.n_workers),
                    Some(&pb),
                )
                .await?;
            pb.finish();
        }
        "upload" => {
            let Some(local_path) = &args.local_path else {
                bail!("local_path is required!");
            };
            client
                .upload_object(
                    &args.bucket,
                    &args.key,
                    local_path,
                    args.storage_class.as_deref(),
                )
                .await?;
        }
        "upload-multipart" => {
            let Some(local_path) = &args.local_path else {
                bail!("local_path is required!");
            };
            let pb = make_progress_bar(Some(0));
            client
                .upload_object_multipart(
                    &args.bucket,
                    &args.key,
                    local_path,
                    Some(args.n_workers),
                    args.storage_class.as_deref(),
                    Some(&pb),
                )
                .await?;
            pb.finish();
        }
        "delete" => {
            client.delete_object(&args.bucket, &args.key).await?;
        }
        "copy" => {
            let Some(src_bucket) = args.src_bucket else {
                bail!("src_bucket is required!");
            };
            let Some(src_key) = args.src_key else {
                bail!("src_key is required!");
            };
            client
                .copy_object(
                    &src_bucket,
                    &src_key,
                    &args.bucket,
                    &args.key,
                    args.storage_class.as_deref(),
                )
                .await?;
        }
        "restore" => {
            client
                .restore_object(
                    &args.bucket,
                    &args.key,
                    args.restore_days,
                    &args.restore_tier,
                )
                .await?;
        }
        _ => {
            panic!("Unknown operation");
        }
    }

    Ok(())
}
