use anyhow::Context;
use chrono::{DateTime, Utc};
use futures::{stream, StreamExt};
use log::{debug, error, info, warn};
use std::time::SystemTime;
use sulfite::S3Client;
use sulfite_tools::utils::{
    get_keys_from_csv, get_line_count, make_progress_bar, warn_prefix_no_trailing_slash,
};

use crate::{CsvArgs, CsvCommand};

pub async fn run_csv(client: S3Client, args: CsvArgs) -> anyhow::Result<()> {
    if let Some(local_dir) = match &args.command {
        CsvCommand::Download { local_dir, .. } => Some(local_dir.clone()),
        _ => None,
    } {
        std::fs::create_dir_all(local_dir)?;
    }

    // Keys are streamed (iterator); we do a separate file read for line count so the progress bar can show total. Memory stays O(1) per key.
    let keys = get_keys_from_csv(&args.source_path, args.column_idx, args.has_header)?;

    match &args.command {
        CsvCommand::Head { prefix, .. }
        | CsvCommand::Download { prefix, .. }
        | CsvCommand::Upload { prefix, .. }
        | CsvCommand::Delete { prefix, .. }
        | CsvCommand::Restore { prefix, .. } => warn_prefix_no_trailing_slash(prefix, "csv"),
        CsvCommand::Copy {
            src_prefix,
            dst_prefix,
            ..
        } => {
            warn_prefix_no_trailing_slash(src_prefix, "csv copy (source)");
            warn_prefix_no_trailing_slash(dst_prefix, "csv copy (destination)");
        }
    }

    let is_head = matches!(&args.command, CsvCommand::Head { .. });
    let pb = if is_head {
        None
    } else {
        let total_lines = get_line_count(&args.source_path)? as u64;
        let key_count = total_lines.saturating_sub(if args.has_header { 1 } else { 0 });
        Some(make_progress_bar(Some(key_count)))
    };

    stream::iter(keys)
        .map(|key_result| {
            let key = match key_result {
                Ok(k) => k,
                Err(e) => {
                    error!("csv key error: {e}");
                    return tokio::spawn(async {});
                }
            };
            let client = client.clone();
            let command = args.command.clone();
            let pb = pb.clone();
            tokio::spawn(async move {
                let res: Result<(), anyhow::Error> = async {
                    match command {
                        CsvCommand::Head { bucket, prefix, suffix, .. } => {
                            let key = format!("{prefix}{key}{suffix}");
                            let obj = client.head_object(&bucket, &key).await
                                .with_context(|| format!("heading key {key}"))?;
                            println!("{obj:?}");
                        }
                        CsvCommand::Download { bucket, prefix, suffix, local_dir, .. } => {
                            let mut local_path = key.clone();
                            local_path = format!("{local_dir}/{local_path}");
                            let key = format!("{prefix}{key}{suffix}");

                            let obj = client.head_object(&bucket, &key).await
                                .with_context(|| format!("heading key {key}"))?;

                            // Skip vs override: if local file exists, compare size and mtime (both as SystemTime).
                            // SystemTime is always a UTC instant (duration since epoch); comparison is timezone-safe.
                            // Skip only when local size == remote size and local timestamp <= remote timestamp (local not newer).
                            if std::path::Path::new(&local_path).exists() {
                                let local_file_info = std::fs::metadata(&local_path)
                                    .with_context(|| format!("reading metadata for {local_path}"))?;
                                let local_file_size = local_file_info.len();
                                let local_mtime = local_file_info.modified()?;
                                let remote_mtime = match SystemTime::try_from(obj.timestamp) {
                                    Ok(t) => t,
                                    Err(e) => {
                                        warn!("key {key}: could not convert remote timestamp to SystemTime ({}), treating as epoch", e);
                                        SystemTime::UNIX_EPOCH
                                    }
                                };
                                if local_file_size == obj.size && local_mtime <= remote_mtime {
                                    if let Some(pb) = pb.as_ref() { pb.set_message(format!("{key} already exists locally. Skipping.")); }
                                    return Ok(());
                                } else {
                                    let local_ts = DateTime::<Utc>::from(local_mtime).format("%Y-%m-%dT%H:%M:%SZ");
                                    info!(
                                        "Object {key} already exists locally but with different size or timestamp.\n  local: {local_file_size} {local_ts}\n  remote: {} {}",
                                        obj.size, obj.timestamp
                                    );
                                }
                            }

                            let dirname = std::path::Path::new(&local_path)
                                .parent()
                                .and_then(|p| p.to_str())
                                .ok_or_else(|| anyhow::anyhow!("path has no parent or invalid UTF-8: {local_path}"))?;

                            tokio::fs::create_dir_all(dirname).await
                                .with_context(|| format!("creating directory {dirname}"))?;

                            // < 1 GB → single GET; >= 1 GB → multipart download.
                            if obj.size < 1024 * 1024 * 1024 {
                                client
                                    .download_object(&bucket, &key, &local_path, None)
                                    .await
                            } else {
                                client
                                    .download_object_multipart(
                                        &bucket,
                                        &key,
                                        &local_path,
                                        None::<&indicatif::ProgressBar>,
                                    )
                                    .await
                            }
                            .with_context(|| format!("downloading object {key}"))?;
                            debug!("object {key} downloaded.");
                        }
                        CsvCommand::Upload { bucket, prefix, suffix, local_dir, storage_class, .. } => {
                            let mut local_path = key.clone();
                            local_path = format!("{local_dir}/{local_path}");
                            let key = format!("{prefix}{key}{suffix}");

                            let local_file_info = std::fs::metadata(&local_path)
                                .with_context(|| format!("reading metadata for {local_path}"))?;
                            let local_file_size = local_file_info.len();
                            let local_mtime = local_file_info.modified()?;

                            // Skip vs override: if remote object exists, compare size and mtime (both as SystemTime).
                            // SystemTime is always a UTC instant; comparison is timezone-safe.
                            // Skip only when local size == remote size and local timestamp <= remote timestamp.
                            if let Ok(obj) = client.head_object(&bucket, key.as_str()).await {
                                let remote_mtime = match SystemTime::try_from(obj.timestamp) {
                                    Ok(t) => t,
                                    Err(e) => {
                                        warn!("key {key}: could not convert remote timestamp to SystemTime ({}), treating as epoch", e);
                                        SystemTime::UNIX_EPOCH
                                    }
                                };
                                if local_file_size == obj.size && local_mtime <= remote_mtime {
                                    if let Some(pb) = pb.as_ref() { pb.set_message(format!("{key} already exists on destination. Skipping.")); }
                                    return Ok(());
                                } else {
                                    let local_ts = DateTime::<Utc>::from(local_mtime).format("%Y-%m-%dT%H:%M:%SZ");
                                    info!(
                                        "Object {key} already exists on destination but with different size or timestamp.\n  local: {local_file_size} {local_ts}\n  remote: {} {}",
                                        obj.size, obj.timestamp
                                    );
                                }
                            }

                            // For archival tier, small files should still be STANDARD for efficiency.
                            // Upload path by size: < 16 KB → single-part, no storage class (default STANDARD);
                            // >= 16 KB and < 1 GB → single-part with storage_class; >= 1 GB → multipart with storage_class.
                            if local_file_size < 16 * 1024 {
                                client.upload_object(&bucket, &key, &local_path, None).await
                            } else if local_file_size < 1024 * 1024 * 1024 {
                                client
                                    .upload_object(&bucket, &key, &local_path, storage_class.as_deref())
                                    .await
                            } else {
                                client
                                    .upload_object_multipart(
                                        &bucket,
                                        &key,
                                        &local_path,
                                        storage_class.as_deref(),
                                        None::<&indicatif::ProgressBar>,
                                    )
                                    .await
                            }
                            .with_context(|| format!("uploading object {key}"))?;
                            debug!("object {key} uploaded.");
                        }
                        CsvCommand::Delete { bucket, prefix, suffix, .. } => {
                            let key = format!("{prefix}{key}{suffix}");
                            client.delete_object(&bucket, &key).await
                                .with_context(|| format!("deleting object {key}"))?;
                            debug!("object {key} deleted.");
                        }
                        CsvCommand::Copy { src_bucket, src_prefix, src_suffix, dst_bucket, dst_prefix, dst_suffix, storage_class, .. } => {
                            let src_key = format!("{src_prefix}{key}{src_suffix}");
                            let dst_key = format!("{dst_prefix}{key}{dst_suffix}");

                            let src_obj = client.head_object(&src_bucket, &src_key).await
                                .with_context(|| format!("heading key {src_key} on source"))?;

                            // Skip only if src and dst are the same key and src is in archival tier.
                            // This happens when you idempotently copy an object into the same destination bucket and key with an archival tier storage class.
                            if let Some(storage_class) = src_obj.storage_class {
                                if src_key == dst_key && (storage_class == "GLACIER" || storage_class == "DEEP_ARCHIVE") {
                                    if let Some(pb) = pb.as_ref() { pb.set_message(format!("{src_key} is in archival tier. Skipping.")); }
                                    return Ok(());
                                }
                            }

                            // For archival tier, small files should still be STANDARD for efficiency.
                            // Copy path by size: < 16 KB → no storage class (default STANDARD);
                            // >= 16 KB → storage class.
                            let copy_storage_class = if src_obj.size < 16 * 1024 {
                                None
                            } else {
                                storage_class.as_deref()
                            };
                            client
                                .copy_object(
                                    &src_bucket,
                                    &src_key,
                                    &dst_bucket,
                                    &dst_key,
                                    copy_storage_class,
                                )
                                .await
                                .with_context(|| format!("copying object {dst_key}"))?;
                            debug!("object {dst_key} copied.");
                        }
                        CsvCommand::Restore { bucket, prefix, suffix, restore_tier, restore_days, .. } => {
                            let key = format!("{prefix}{key}{suffix}");
                            client
                                .restore_object(&bucket, &key, restore_days, &restore_tier)
                                .await
                                .with_context(|| format!("restoring object {key}"))?;
                            debug!("object {key} restored.");
                        }
                    }
                    Ok(())
                }
                .await;

                if let Err(e) = res {
                    error!("{e:#}");
                }
                if let Some(pb) = pb.as_ref() { pb.inc(1) }
            })
        })
        .buffer_unordered(if is_head { 1 } else { args.n_workers })
        .for_each(|r| async {
            if let Err(e) = r {
                error!("Spawned task failed: {e:#}");
            }
        })
        .await;

    if let Some(pb) = pb.as_ref() { pb.finish() }

    Ok(())
}
