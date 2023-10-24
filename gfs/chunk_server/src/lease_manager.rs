use std::time::{Duration, SystemTime, UNIX_EPOCH};

use common::concurrent_hash_map::ConcurrentHashMap;

#[derive(Debug)]
pub struct LeaseManager {
    /// chunk_handle -> expiration_time (from UNIX epoch)
    leases: ConcurrentHashMap<String, SystemTime>,
}

impl LeaseManager {
    pub fn new() -> Self {
        LeaseManager {
            leases: ConcurrentHashMap::new(),
        }
    }

    pub fn grant_lease(&self, chunk_handle: String, expiration_unix_sec: u64) {
        let expiration = UNIX_EPOCH + Duration::from_secs(expiration_unix_sec);
        self.leases.insert(chunk_handle, expiration);
    }

    pub fn revoke_lease(&self, chunk_handle: &String) {
        self.leases.remove(chunk_handle);
    }

    pub fn has_valid_lease(&self, chunk_handle: &String) -> bool {
        let lease = self.leases.get(chunk_handle);

        match lease {
            None => false,
            Some(expiration_time) if SystemTime::now() > expiration_time => {
                // Removed expired lease.
                self.leases.remove(chunk_handle);
                false
            }
            Some(_) => true,
        }
    }
}

#[cfg(test)]
mod lease_manager_test {
    use std::ops::Add;
    use std::thread;

    use super::*;

    // Create a lease expires that is in $timeout_sec seconds from now.
    macro_rules! create_lease {
        ($timeout_sec:expr) => {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .add(Duration::from_secs($timeout_sec))
                .as_secs()
        };
    }

    #[test]
    fn lease_expires_test() {
        let lease_manager = LeaseManager::new();
        let chunk_handle = String::from("chunk_handle");
        let expiration_unix_sec = create_lease!(1);

        lease_manager.grant_lease(chunk_handle.clone(), expiration_unix_sec);

        // Sleep sleeps for a bit over a second.
        thread::sleep(Duration::from_secs_f32(1.1));

        assert!(!lease_manager.has_valid_lease(&chunk_handle));
    }

    #[test]
    fn revoke_lease_test() {
        let lease_manager = LeaseManager::new();
        let chunk_handle = String::from("chunk_handle");
        let expiration_unix_sec = create_lease!(9999999);

        lease_manager.grant_lease(chunk_handle.clone(), expiration_unix_sec);
        lease_manager.revoke_lease(&chunk_handle);

        assert!(!lease_manager.has_valid_lease(&chunk_handle));
    }

    #[test]
    fn has_valid_lease_test() {
        let lease_manager = LeaseManager::new();
        let chunk_handle = String::from("chunk_handle");
        let expiration_unix_sec = create_lease!(10);

        lease_manager.grant_lease(chunk_handle.clone(), expiration_unix_sec);

        assert!(lease_manager.has_valid_lease(&chunk_handle));
    }
}
