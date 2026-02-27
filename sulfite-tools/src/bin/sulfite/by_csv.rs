use anyhow::Result;
use chrono::{DateTime, Utc};
use futures::{stream, StreamExt};
use log::{debug, error, info};
use sulfite::{RetryConfig, S3Client, S3ClientConfig};
use sulfite_tools::utils::make_progress_bar;

use crate::ByCsvArgs;

fn get_keys_from_csv(
    filepath: &str,
    column_index: usize,
    has_header: bool,
) -> csv::Result<Vec<String>> {
    let mut keys = vec![];
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(has_header)
        .from_path(filepath)?;

    for r in rdr.records() {
        let r = r?;
        let key = r[column_index].to_string();
        keys.push(key);
    }
    info!("Keys count: {}", keys.len());
    Ok(keys)
}

pub async fn run_by_csv(
    region: &Option<String>,
    endpoint_url: &Option<String>,
    args: ByCsvArgs,
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

    if let Some(local_dir) = &args.local_dir {
        std::fs::create_dir_all(local_dir)?;
    }

    let keys = get_keys_from_csv(&args.source_path, args.column_idx, args.has_header)?;

    // make pretty progress bar
    let pb = match args.op.as_str() {
        "head" => None,
        _ => {
            let pb = make_progress_bar(Some(keys.len() as u64));
            Some(pb)
        }
    };

    stream::iter(keys)
        .map(|key| {
            let client = client.clone();
            let bucket = args.bucket.clone();
            let prefix = args.prefix.clone();
            let suffix = args.suffix.clone();
            let op = args.op.clone();
            let pb = pb.clone();
            let local_dir = args.local_dir.clone();
            let src_bucket = args.src_bucket.clone();
            let src_prefix = args.src_prefix.clone();
            let src_suffix = args.src_suffix.clone();
            let storage_class = args.storage_class.clone();
            let restore_tier = args.restore_tier.clone();
            let restore_days = args.restore_days;
            let n_inner_workers = args.n_inner_workers;
            tokio::spawn(async move {
                match op.as_str() {
                    "head" => {
                        let key = format!("{prefix}{key}{suffix}");
                        let res = client.head_object(&bucket, &key).await;
                        if let Ok(obj) = res {
                            println!("{obj:?}");
                        } else if let Err(e) = res {
                            error!("Error heading key {key}: {e}");
                        }
                    }
                    "download" => {
                        // we reproduce the directory structure
                        let mut local_path = key.clone();
                        if let Some(local_dir) = local_dir {
                            local_path = format!("{local_dir}/{local_path}");
                        }
                        let key = format!("{prefix}{key}{suffix}");

                        let Ok(obj) = client.head_object(&bucket, &key).await else {
                            error!("Object {key} not found.");
                            return;
                        };

                        if std::path::Path::new(&local_path).exists() {
                            let local_file_info = std::fs::metadata(&local_path).unwrap();
                            let local_file_size = local_file_info.len();
                            let local_file_timestamp =
                                DateTime::<Utc>::from(local_file_info.modified().unwrap())
                                    .format("%Y-%m-%dT%H:%M:%SZ")
                                    .to_string();
                            if local_file_size == obj.size && local_file_timestamp <= obj.timestamp.to_string()
                            {
                                pb.as_ref().map(|pb| {
                                    pb.set_message(format!("{key} already exists locally."));
                                    pb.inc(1);
                                });
                                return;
                            } else {
                                info!(
                                    "Object {key} already exists locally but with different size or timestamp.\n  local: {local_file_size} {local_file_timestamp}\n  remote: {} {}",
                                    obj.size, obj.timestamp.to_string()
                                );
                            }
                        }

                        // get parent dirname
                        let dirname = std::path::Path::new(&local_path)
                            .parent()
                            .and_then(|os_str| os_str.to_str())
                            .unwrap()
                            .to_owned();

                        // create parent dir
                        if let Err(e) = tokio::fs::create_dir_all(&dirname).await {
                            error!("Error creating directory {dirname}: {e}");
                        };

                        let res = if obj.size < 1 * 1024 * 1024 * 1024 {
                            // if the object is less than 1GB, we use the normal download object
                            client
                                .download_object(&bucket, &key, &local_path, None)
                                .await
                        } else {
                            // if the object is greater than 1GB, we use the multipart download object
                            client
                                .download_object_multipart(
                                    &bucket,
                                    &key,
                                    &local_path,
                                    Some(n_inner_workers),
                                    None,
                                )
                                .await
                        };
                        if let Ok(_) = res {
                            debug!("object {key} downloaded.");
                        } else if let Err(e) = res {
                            error!("Error downloading object {key}: {e}");
                        }
                    }
                    "download-flatten" => {
                        // we flatten the directory structure when storing locally
                        let mut local_path = std::path::Path::new(&key)
                            .file_name()
                            .and_then(|os_str| os_str.to_str())
                            .unwrap()
                            .to_owned();
                        if let Some(local_dir) = local_dir {
                            local_path = format!("{local_dir}/{local_path}");
                        }
                        let key = format!("{prefix}{key}{suffix}");

                        let Ok(obj) = client.head_object(&bucket, &key).await else {
                            error!("Object {key} not found.");
                            return;
                        };

                        if std::path::Path::new(&local_path).exists() {
                            let local_file_info = std::fs::metadata(&local_path).unwrap();
                            let local_file_size = local_file_info.len();
                            let local_file_timestamp =
                                DateTime::<Utc>::from(local_file_info.modified().unwrap())
                                    .format("%Y-%m-%dT%H:%M:%SZ")
                                    .to_string();
                            if local_file_size == obj.size && local_file_timestamp <= obj.timestamp.to_string()
                            {
                                pb.as_ref().map(|pb| {
                                    pb.set_message(format!("{key} already exists locally."));
                                    pb.inc(1);
                                });
                                return;
                            } else {
                                info!(
                                    "Object {key} already exists locally but with different size or timestamp.\n  local: {local_file_size} {local_file_timestamp}\n  remote: {} {}",
                                    obj.size, obj.timestamp.to_string()
                                );
                            }
                        }

                        let res = if obj.size < 1 * 1024 * 1024 * 1024 {
                            // if the object is less than 1GB, we use the normal download object
                            client
                                .download_object(&bucket, &key, &local_path, None)
                                .await
                        } else {
                            // if the object is greater than 1GB, we use the multipart download object
                            client
                                .download_object_multipart(
                                    &bucket,
                                    &key,
                                    &local_path,
                                    Some(n_inner_workers),
                                    None,
                                )
                                .await
                        };
                        if let Ok(_) = res {
                            debug!("object {key} downloaded.");
                        } else if let Err(e) = res {
                            error!("Error downloading object {key}: {e}");
                        }
                    }
                    "upload" => {
                        // we create a directory structure at the prefix
                        // key needs to be relative to local_dir
                        let mut local_path = key.clone();
                        if let Some(local_dir) = local_dir {
                            local_path = format!("{local_dir}/{local_path}");
                        }
                        let key = format!("{prefix}{key}{suffix}");

                        // check local filesize and timestamp
                        let local_file_info = std::fs::metadata(&local_path).unwrap();
                        let local_file_size = local_file_info.len();
                        // format to 2019-12-16T23:48:18Z
                        let local_file_timestamp =
                            DateTime::<Utc>::from(local_file_info.modified().unwrap())
                                .format("%Y-%m-%dT%H:%M:%SZ")
                                .to_string();

                        if let Ok(obj) = client.head_object(&bucket, key.as_str()).await {
                            if obj.size == local_file_size && obj.timestamp.to_string() >= local_file_timestamp
                            {
                                pb.as_ref().map(|pb| {
                                    pb.set_message(format!("{key} already exists on destination."));
                                    pb.inc(1);
                                });
                                return;
                            } else {
                                info!(
                                    "Object {key} already exists on destination but with different size or timestamp.\n  local: {local_file_size} {local_file_timestamp}\n  remote: {} {}",
                                    obj.size, obj.timestamp.to_string()
                                );
                            }
                        }

                        let res = if local_file_size < 16 * 1024 {
                            // if the object is less than 16KB, we don't need to set the storage class to use default - STANDARD
                            client.upload_object(&bucket, &key, &local_path, None).await
                        } else if local_file_size < 1 * 1024 * 1024 * 1024 {
                            // if the object is less than 1GB, we use the normal upload object
                            client
                                .upload_object(&bucket, &key, &local_path, storage_class.as_deref())
                                .await
                        } else {
                            // if the object is greater than 1GB, we use the multipart upload
                            client
                                .upload_object_multipart(
                                    &bucket,
                                    &key,
                                    &local_path,
                                    Some(n_inner_workers),
                                    storage_class.as_deref(),
                                    None,
                                )
                                .await
                        };
                        if let Ok(_) = res {
                            debug!("object {key} uploaded.");
                        } else if let Err(e) = res {
                            error!("Error uploading object {key}: {e}");
                        }
                    }
                    "delete" => {
                        let key = format!("{prefix}{key}{suffix}");
                        let res = client.delete_object(&bucket, &key).await;
                        if let Ok(_) = res {
                            debug!("object {key} deleted.");
                        } else if let Err(e) = res {
                            error!("Error deleting object {key}: {e}");
                        }
                    }
                    "copy" => {
                        let Some(src_bucket) = src_bucket else {
                            error!("src_bucket is required!");
                            return;
                        };
                        let src_key = format!("{src_prefix}{key}{src_suffix}");
                        let key = format!("{prefix}{key}{suffix}");

                        let Ok(obj) = client.head_object(&src_bucket, &src_key).await else {
                            error!("Object {src_key} not found on source account.");
                            return;
                        };

                        let storage_class = if obj.size < 16 * 1024 {
                            None
                        } else {
                            storage_class
                        };
                        let res = client
                            .copy_object(
                                &src_bucket,
                                &src_key,
                                &bucket,
                                &key,
                                storage_class.as_deref(),
                            )
                            .await;
                        if let Ok(_) = res {
                            debug!("object {key} copied.");
                        } else if let Err(e) = res {
                            if !e.to_string().contains("AccessDenied") {
                                // this happens when the source is already in archival tier
                                error!("Error copying object {key}: {e}");
                            }
                        }
                    }
                    "restore" => {
                        let key = format!("{prefix}{key}{suffix}");
                        let res = client
                            .restore_object(&bucket, &key, restore_days, &restore_tier)
                            .await;
                        if let Ok(_) = res {
                            debug!("object {key} restored.");
                        } else if let Err(e) = res {
                            error!("Error restoring object {key}: {e}");
                        }
                    }
                    _ => {
                        error!("Operation not supported: {op}");
                    }
                }
                pb.as_ref().map(|pb| {
                    pb.inc(1);
                });
            })
        })
        .buffer_unordered(if args.op.as_str() == "head" {
            // we don't need multiple workers here
            1
        } else {
            args.n_workers
        })
        .for_each(|_| async {})
        .await;

    Ok(())
}
