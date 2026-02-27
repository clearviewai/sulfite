use anyhow::Result;
use colored::Colorize;
use sulfite::{RetryConfig, S3Client, S3ClientConfig};

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

    // list objects
    let (objects, common_prefixes) = client
        .list_objects_v2_paginate(&args.bucket, &args.prefix, args.delimiter.as_deref())
        .await?;

    let prefix = &args.prefix;
    let suffix = &args.suffix;
    let keep_prefix = args.keep_prefix;
    let remove_suffix = args.remove_suffix;

    let objects = objects
        .into_iter()
        .filter(|object| object.key.ends_with(suffix))
        .collect::<Vec<_>>();

    if let Some(output_path) = args.output_path {
        let mut writer = csv::Writer::from_path(output_path)?;
        writer.write_record(&["key", "size", "timestamp", "storage_class"])?; // write header
        objects.iter().for_each(|object| {
            let mut key = object.key.clone();
            if !keep_prefix {
                key = key.replace(prefix, "");
            }
            if remove_suffix {
                key = key.replace(suffix, "");
            }
            writer
                .write_record(&[
                    key.as_str(),
                    object.size.to_string().as_str(),
                    object.timestamp.to_string().as_str(),
                    object.storage_class.as_ref().unwrap(),
                ])
                .unwrap();
        });
        writer.flush()?;
    }

    println!("{}", format!("Found {} objects.", objects.len()).bold());
    if !objects.is_empty() {
        println!(
            "{}",
            format!(
                "Listing first {}...",
                std::cmp::min(args.head, objects.len())
            )
            .italic()
            .underline()
        );
    }
    objects.iter().take(args.head).for_each(|object| {
        println!("  {}", object.key.replace(prefix, "").bold());
        println!(
            "    {} {} {} {} {} {}",
            "size:".blue(),
            object.size,
            "timestamp:".blue(),
            object.timestamp,
            "storage_class:".blue(),
            object.storage_class.as_ref().unwrap()
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
