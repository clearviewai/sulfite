use indicatif::{ProgressBar, ProgressStyle};
use std::io::BufRead;

/// Warns if a non-empty prefix does not end with '/'. Directory-style S3 keys usually
/// use a trailing slash; omitting it can yield unexpected matches (e.g. "foo" matches
/// "foo" and "fooBar"). Not appropriate when the prefix is intentional and non-path
/// (e.g. "archive-" or "year-2024-"); in those cases the user can ignore the warning.
pub fn warn_prefix_no_trailing_slash(prefix: &str, context: &str) {
    if !prefix.is_empty() && !prefix.ends_with('/') {
        eprintln!(
            "WARNING [{}]: Prefix does not end with '/'. Keys may not match directory-style paths. \
             (if intentional, e.g. non-path prefix like 'archive-', ignore.)",
            context
        );
    }
}

pub fn get_line_count(filepath: &str) -> std::io::Result<usize> {
    let reader = std::io::BufReader::new(std::fs::File::open(filepath)?);
    let lines = reader.lines();
    Ok(lines.count())
}

pub fn get_keys_from_csv(
    filepath: &str,
    column_index: usize,
    has_header: bool,
) -> csv::Result<impl Iterator<Item = Result<String, csv::Error>>> {
    let rdr = csv::ReaderBuilder::new()
        .has_headers(has_header)
        .from_path(filepath)?;

    Ok(rdr
        .into_records()
        .map(move |record| record.map(|r| r[column_index].to_string())))
}

pub fn make_progress_bar(total: Option<u64>) -> indicatif::ProgressBar {
    let pb;
    let sty;
    match total {
        Some(total) => {
            pb = ProgressBar::new(total);
            sty = ProgressStyle::with_template(
                "{spinner:.cyan} [{bar:40.cyan/blue}] {pos:>7}/{len:7} [{elapsed_precise}<{eta_precise} {per_sec:.green}] {msg}"
            )
            .expect("valid progress bar template")
            .progress_chars("#>-");
        }
        None => {
            pb = ProgressBar::new_spinner();
            sty = ProgressStyle::with_template(
                "{spinner:.cyan} {pos:>7} [{elapsed_precise} {per_sec:.green}]",
            )
            .expect("valid progress bar template")
            .progress_chars("#>-");
        }
    }
    pb.set_style(sty);
    pb
}
