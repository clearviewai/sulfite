use colored::Colorize;
use sulfite::S3Client;
use sulfite_tools::utils::warn_prefix_no_trailing_slash;

use crate::ListArgs;

pub async fn run_list(client: S3Client, args: ListArgs) -> anyhow::Result<()> {
    warn_prefix_no_trailing_slash(&args.prefix, "list");
    let prefix = &args.prefix;
    let suffix = &args.suffix;
    let keep_prefix = args.keep_prefix;
    let remove_suffix = args.remove_suffix;

    let mut object_count: usize = 0;
    let mut objects_to_display = vec![];
    let mut prefix_count: usize = 0;
    let mut prefixes_to_display = vec![];

    let mut writer = match &args.output_path {
        Some(p) => {
            let mut w = csv::Writer::from_path(p)?;
            w.write_record(&["key", "size", "timestamp", "storage_class"])?;
            Some(w)
        }
        None => None,
    };

    let mut pages =
        client.list_objects_v2_page_iter(&args.bucket, &args.prefix, Some(args.delimiter.as_str()));

    while let Some((objs, prefixes)) = pages.next_page().await? {
        for obj in objs {
            if !obj.key.ends_with(suffix) {
                continue;
            }
            object_count += 1;
            if let Some(w) = &mut writer {
                let mut key = obj.key.clone();
                if !keep_prefix {
                    if let Some(s) = key.strip_prefix(prefix.as_str()) {
                        key = s.to_string();
                    }
                }
                if remove_suffix {
                    if let Some(s) = key.strip_suffix(suffix) {
                        key = s.to_string();
                    }
                }
                let _ = w.write_record(&[
                    key.as_str(),
                    obj.size.to_string().as_str(),
                    obj.timestamp.to_string().as_str(),
                    obj.storage_class.as_deref().unwrap_or(""),
                ]);
            }
            if objects_to_display.len() < args.display_max_entries {
                objects_to_display.push(obj);
            }
        }

        for p in prefixes {
            if !p.prefix.ends_with(suffix) {
                continue;
            }
            prefix_count += 1;
            if prefixes_to_display.len() < args.display_max_entries {
                prefixes_to_display.push(p);
            }
        }
    }

    if let Some(w) = &mut writer {
        w.flush()?;
    }

    println!("{}", format!("Found {} objects.", object_count).bold());
    if !objects_to_display.is_empty() {
        println!(
            "{}",
            format!("Listing first {}...", objects_to_display.len())
                .italic()
                .underline()
        );
    }
    // Console display always strips the list prefix for readability (keep_prefix only affects CSV output).
    objects_to_display.iter().for_each(|obj| {
        if let Some(s) = obj.key.strip_prefix(prefix.as_str()) {
            println!("  {}", s.to_string().bold());
        } else {
            // should never happen
            println!("  {}", obj.key.bold());
        }
        let size_kb = obj.size as f64 / 1024.0;
        let size_mb = size_kb / 1024.0;
        let size_gb = size_mb / 1024.0;
        let size_tb = size_gb / 1024.0;
        let size_human_str = if size_tb > 1.0 {
            format!(" ({:.2}T)", size_tb)
        } else if size_gb > 1.0 {
            format!(" ({:.2}G)", size_gb)
        } else if size_mb > 1.0 {
            format!(" ({:.2}M)", size_mb)
        } else {
            format!(" ({:.2}K)", size_kb)
        };
        println!(
            "    {} {}{} {} {} {} {}",
            "size:".blue(),
            obj.size,
            size_human_str,
            "timestamp:".blue(),
            obj.timestamp,
            "storage_class:".blue(),
            obj.storage_class.as_deref().unwrap_or("")
        );
    });

    println!("{}", "-----------------------------------".bold());

    println!(
        "{}",
        format!("Found {} common prefixes.", prefix_count).bold()
    );
    if !prefixes_to_display.is_empty() {
        println!(
            "{}",
            format!("Listing first {}...", prefixes_to_display.len())
                .italic()
                .underline()
        );
    }
    prefixes_to_display.iter().for_each(|p| {
        if let Some(s) = p.prefix.strip_prefix(prefix.as_str()) {
            println!("  {}", s.to_string().bold());
        } else {
            // should never happen
            println!("  {}", p.prefix.bold());
        }
    });

    Ok(())
}
