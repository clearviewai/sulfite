use anyhow::Result;
use colored::Colorize;
use sulfite::{ObjectInfo, RetryConfig, S3Client, S3ClientConfig};

use crate::ListArgs;

pub async fn run_list(
    region: &Option<String>,
    endpoint_url: &Option<String>,
    args: ListArgs,
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

    let prefix = &args.prefix;
    let suffix = &args.suffix;
    let keep_prefix = args.keep_prefix;
    let remove_suffix = args.remove_suffix;

    let mut object_count: usize = 0;
    let mut head_objects: Vec<ObjectInfo> = Vec::new();
    let mut common_prefixes = Vec::new();

    let mut writer = match &args.output_path {
        Some(p) => {
            let mut w = csv::Writer::from_path(p)?;
            w.write_record(&["key", "size", "timestamp", "storage_class"])?;
            Some(w)
        }
        None => None,
    };

    let mut pages = client.list_objects_v2_page_iter(
        &args.bucket,
        &args.prefix,
        args.delimiter.as_deref(),
    );

    while let Some((objs, mut prefixes)) = pages.next_page().await? {
        common_prefixes.append(&mut prefixes);
        for object in objs {
            if !object.key.ends_with(suffix) {
                continue;
            }
            object_count += 1;
            if let Some(w) = &mut writer {
                let mut key = object.key.clone();
                if !keep_prefix {
                    key = key.replace(prefix, "");
                }
                if remove_suffix {
                    key = key.replace(suffix, "");
                }
                let _ = w.write_record(&[
                    key.as_str(),
                    object.size.to_string().as_str(),
                    object.timestamp.to_string().as_str(),
                    object.storage_class.as_deref().unwrap_or(""),
                ]);
            }
            if head_objects.len() < args.head {
                head_objects.push(object);
            }
        }
    }

    if let Some(w) = &mut writer {
        w.flush()?;
    }

    println!("{}", format!("Found {} objects.", object_count).bold());
    if !head_objects.is_empty() {
        println!(
            "{}",
            format!(
                "Listing first {}...",
                std::cmp::min(args.head, head_objects.len())
            )
            .italic()
            .underline()
        );
    }
    head_objects.iter().for_each(|object| {
        println!("  {}", object.key.replace(prefix, "").bold());
        println!(
            "    {} {} {} {} {} {}",
            "size:".blue(),
            object.size,
            "timestamp:".blue(),
            object.timestamp,
            "storage_class:".blue(),
            object.storage_class.as_deref().unwrap_or("")
        );
    });

    println!("{}", "-----------------------------------".bold());

    println!(
        "{}",
        format!("Found {} common prefixes.", common_prefixes.len()).bold()
    );
    if !common_prefixes.is_empty() {
        println!("{}", "Listing...".italic().underline());
    }
    common_prefixes.iter().for_each(|common_prefix| {
        println!("  {}", common_prefix.prefix.replace(prefix, "").bold());
    });

    Ok(())
}
