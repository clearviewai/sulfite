use anyhow::{bail, Context, Result};
use sulfite::{RetryConfig, S3Client, S3ClientConfig};
use sulfite_tools::utils::make_progress_bar;

use crate::OpArgs;

pub async fn run_op(
    region: &Option<String>,
    endpoint_url: &Option<String>,
    args: OpArgs,
) -> Result<()> {
    let client = S3Client::new(
        S3ClientConfig {
            region: region.clone(),
            endpoint_url: endpoint_url.clone(),
            ..Default::default()
        },
        RetryConfig {
            max_retries: 10,
            ..Default::default()
        },
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
