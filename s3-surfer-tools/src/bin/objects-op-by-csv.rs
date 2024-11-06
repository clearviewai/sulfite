use anyhow::Result;
use clap::Parser;
use futures::{stream, StreamExt};
use log::{debug, error, info};
use s3_surfer::s3_client;
use s3_surfer::utils::make_progress_bar;

#[derive(Parser)]
struct Cli {
    /// Operation: head, download, download-flatten (flattens filepaths), upload, delete, copy, restore
    op: String,
    /// Input csv path
    source_path: String,
    /// column index
    #[clap(short, long, default_value = "0")]
    column_idx: usize,
    /// has header
    #[clap(long, default_value = "false")]
    has_header: bool,
    /// local dir for downloads and uploads
    #[clap(short, long)]
    local_dir: Option<String>,
    /// The bucket name
    #[clap(short, long)]
    bucket: String,
    /// The prefix before provided keys
    #[clap(short, long)]
    prefix: String,
    /// The suffix after provided keys
    #[clap(short, long, default_value = "")]
    suffix: String,
    /// src_bucket: source bucket for copy operation
    #[clap(long)]
    src_bucket: Option<String>,
    /// The source prefix before provided keys
    #[clap(long, default_value = "")]
    src_prefix: String,
    /// The source suffix after provided keys
    #[clap(long, default_value = "")]
    src_suffix: String,
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
    /// n workers
    #[clap(short, long, default_value = "250")]
    n_workers: usize,
}

pub fn get_keys_from_csv(
    filepath: &str,
    column_index: usize,
    has_header: bool,
) -> csv::Result<Vec<String>> {
    let mut keys = vec![];
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(has_header)
        .from_path(filepath)?;

    // looks like
    // 31972,facesearch/2023/12/14/21/018c659f-7c00-7bad-a98a-37b42f73f228.tar
    // or other formats, with the key in a column

    for r in rdr.records() {
        let r = r?;
        let key = r[column_index].to_string();
        keys.push(key);
    }
    info!("Keys count: {}", keys.len());
    Ok(keys)
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    let args = Cli::parse();

    let client = s3_client::S3Client::new(
        args.region,
        args.endpoint_url,
        None,
        None,
        None,
        None,
        Some(10),
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
            let restore_days = args.restore_days.clone();
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

                        // skip if exists
                        if std::path::Path::new(&local_path).exists() {
                            pb.as_ref().map(|pb| {
                                pb.set_message(format!("{key} already exists locally."));
                                pb.inc(1);
                            });
                            return;
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

                        let res = client
                            .download_object(&bucket, &key, &local_path, None)
                            .await;
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

                        // skip if exists
                        if std::path::Path::new(&local_path).exists() {
                            pb.as_ref().map(|pb| {
                                pb.set_message(format!("{key} already exists locally."));
                                pb.inc(1);
                            });
                            return;
                        }

                        let res = client
                            .download_object(&bucket, &key, &local_path, None)
                            .await;
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

                        // skip if exists
                        if client.head_object(&bucket, key.as_str()).await.is_ok() {
                            pb.as_ref().map(|pb| {
                                pb.set_message(format!("{key} already exists on destination."));
                                pb.inc(1);
                            });
                            return;
                        }

                        let res = client
                            .upload_object(
                                &bucket,
                                key.as_str(),
                                local_path.as_str(),
                                storage_class.as_deref(),
                            )
                            .await;
                        if let Ok(_) = res {
                            debug!("object {key} uploaded.");
                        } else if let Err(_) = res {
                            let res = client
                                .upload_object_multipart(
                                    &bucket,
                                    key.as_str(),
                                    local_path.as_str(),
                                    Some(10),
                                    storage_class.as_deref(),
                                    None,
                                )
                                .await;
                            if let Ok(_) = res {
                                debug!("object {key} uploaded.");
                            } else if let Err(e) = res {
                                error!("Error uploading object {key}: {e}");
                            }
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
        .buffer_unordered(args.n_workers) // much faster
        .for_each(|_| async {})
        .await;

    Ok(())
}
