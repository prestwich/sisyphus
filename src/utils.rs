use rand::{rngs::ThreadRng, Rng};

// adds up to a second of random delay to cause production tasks to not be synced
pub fn noisy_sleep(approx_millis: u64) -> tokio::time::Sleep {
    let noise = ThreadRng::default().gen_range(0..1000u64);
    let duration = std::time::Duration::from_millis(approx_millis + noise);
    tokio::time::sleep(duration)
}
