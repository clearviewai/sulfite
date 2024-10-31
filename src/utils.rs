use rand::Rng;

pub fn generate_random_hex(n_digits: usize) -> String {
    let mut rng = rand::thread_rng();
    (0..n_digits)
        .map(|_| format!("{:x}", rng.gen_range(0..16)))
        .collect()
}
