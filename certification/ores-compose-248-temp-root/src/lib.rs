use std::env;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

static TEST_TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(1);

pub fn temp_root() -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock must follow unix epoch")
        .as_nanos();
    let sequence = TEST_TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let root = env::temp_dir().join(format!(
        "ores-compose-up-test-{}-{nonce}-{sequence}",
        std::process::id()
    ));
    fs::create_dir_all(&root).expect("create temp root");
    fs::canonicalize(root).expect("canonicalize temp root")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::sync::{Arc, Mutex};
    use std::thread;

    const UPSTREAM: &str = include_str!("../upstream-local_up_process.rs");

    #[test]
    fn harness_matches_upstream_temp_root_shape() {
        assert!(UPSTREAM.contains("static TEST_TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(1);"));
        assert!(
            UPSTREAM.contains("let sequence = TEST_TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);")
        );
        assert!(UPSTREAM.contains("\"ores-compose-up-test-{}-{nonce}-{sequence}\","));
        assert!(UPSTREAM.contains("std::process::id()"));
    }

    #[test]
    fn temp_roots_are_unique_under_parallel_threads() {
        const THREADS: usize = 64;
        const ROOTS_PER_THREAD: usize = 200;
        let paths = Arc::new(Mutex::new(Vec::with_capacity(THREADS * ROOTS_PER_THREAD)));

        let mut handles = Vec::with_capacity(THREADS);
        for _ in 0..THREADS {
            let paths = Arc::clone(&paths);
            handles.push(thread::spawn(move || {
                let mut local = Vec::with_capacity(ROOTS_PER_THREAD);
                for _ in 0..ROOTS_PER_THREAD {
                    local.push(temp_root());
                }
                paths.lock().expect("path lock").extend(local);
            }));
        }
        for handle in handles {
            handle.join().expect("worker thread");
        }

        let paths = paths.lock().expect("path lock");
        assert_eq!(paths.len(), THREADS * ROOTS_PER_THREAD);
        let unique: HashSet<_> = paths.iter().collect();
        assert_eq!(
            unique.len(),
            paths.len(),
            "parallel temp-root allocations must never collide"
        );
    }

    #[test]
    fn temp_root_names_bind_process_and_sequence_components() {
        let path = temp_root();
        let name = path
            .file_name()
            .and_then(|value| value.to_str())
            .expect("utf-8 temp path");
        assert!(name.starts_with("ores-compose-up-test-"));
        assert!(
            name.contains(&format!("ores-compose-up-test-{}-", std::process::id())),
            "temp-root name must bind the process id"
        );
        assert!(
            name.rsplit('-')
                .next()
                .is_some_and(|part| part.parse::<u64>().is_ok()),
            "temp-root name must end in an atomic sequence"
        );
    }
}
