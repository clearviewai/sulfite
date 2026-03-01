use rand::RngExt;

/// Returns a string of `n_digits` random lowercase hex characters (e.g. for temp file suffixes).
pub fn generate_random_hex(n_digits: usize) -> String {
    let mut rng = rand::rng();
    (0..n_digits)
        .map(|_| format!("{:x}", rng.random_range(0..16)))
        .collect()
}
