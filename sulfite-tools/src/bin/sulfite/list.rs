use colored::Colorize;
use sulfite::S3Client;
use sulfite_tools::utils::{print_object_human, warn_prefix_no_trailing_slash};

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
            w.write_record(["key", "size", "timestamp", "storage_class"])?;
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
                if !keep_prefix && let Some(s) = key.strip_prefix(prefix.as_str()) {
                    key = s.to_string();
                }
                if remove_suffix && let Some(s) = key.strip_suffix(suffix) {
                    key = s.to_string();
                }
                let _ = w.write_record([
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
        let display_key = obj.key.strip_prefix(prefix.as_str()).unwrap_or(&obj.key);
        print_object_human(display_key, obj);
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
