use anyhow::Result;
use clap::Parser;
use colored::Colorize;
use sulfite::s3_client;

#[derive(Parser)]
struct Cli {
    /// The bucket name
    #[clap(short, long)]
    bucket: String,
    /// The prefix in a bucket
    #[clap(short, long)]
    prefix: String,
    /// The suffix name for records storage
    #[clap(short, long, default_value = "")]
    suffix: String,
    /// The delimiter, default to '/' for ease and consistency with aws-cli
    #[clap(short, long, default_value = "/")]
    delimiter: Option<String>,
    /// head
    #[clap(long, default_value = "10")]
    head: usize,
    /// Output dir
    #[clap(short, long)]
    output_path: Option<String>,
    /// keep the prefix from the key in the output file - by default it is removed
    #[clap(long, default_value = "false")]
    keep_prefix: bool,
    /// remove the suffix from the key in the output file - by default it is kept
    #[clap(long, default_value = "false")]
    remove_suffix: bool,
    /// The region
    #[clap(short, long)]
    region: Option<String>,
    /// The endpoint URL
    #[clap(short, long)]
    endpoint_url: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    let args = Cli::parse();

    let client =
        s3_client::S3Client::new(args.region, args.endpoint_url, None, None, None, None, None)
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
    if objects.len() > 0 {
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
    if common_prefixes.len() > 0 {
        println!("{}", format!("Listing...",).italic().underline());
    }
    common_prefixes.iter().for_each(|common_prefix| {
        println!("  {}", common_prefix.prefix.replace(prefix, "").bold());
    });

    Ok(())
}
