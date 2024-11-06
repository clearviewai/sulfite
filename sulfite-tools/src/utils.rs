use indicatif::{ProgressBar, ProgressStyle};

pub fn make_progress_bar(total: Option<u64>) -> indicatif::ProgressBar {
    let pb;
    let sty;
    match total {
        Some(total) => {
            pb = ProgressBar::new(total);
            sty = ProgressStyle::with_template(
                "{spinner:.cyan} [{bar:40.cyan/blue}] {pos:>7}/{len:7} [{elapsed_precise}<{eta_precise} {per_sec:.green}] {msg}"
            )
            .unwrap()
            .progress_chars("#>-");
        }
        None => {
            pb = ProgressBar::new_spinner();
            sty = ProgressStyle::with_template(
                "{spinner:.cyan} {pos:>7} [{elapsed_precise} {per_sec:.green}]",
            )
            .unwrap()
            .progress_chars("#>-");
        }
    }
    pb.set_style(sty);
    pb
}
