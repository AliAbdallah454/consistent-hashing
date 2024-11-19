use std::hash::{BuildHasher, Hasher};

#[derive(Debug, Clone)]
pub struct IdentityHasher {
    hash: u64,
}

impl IdentityHasher {
    fn new() -> Self {
        IdentityHasher { hash: 0 }
    }
}

impl Hasher for IdentityHasher {
    fn write(&mut self, bytes: &[u8]) {
        if let Ok(s) = std::str::from_utf8(bytes) {
            self.hash = s.parse::<u64>().expect("This should never fail");
        }
    }

    fn finish(&self) -> u64 {
        self.hash
    }
}

impl Default for IdentityHasher {
    fn default() -> Self {
        IdentityHasher::new()
    }
}

#[derive(Debug, Clone)]
struct IdentityHasherBuilder;   

impl BuildHasher for IdentityHasherBuilder {
    type Hasher = IdentityHasher;

    fn build_hasher(&self) -> Self::Hasher {
        IdentityHasher::new()
    }
}


// use std::hash::{BuildHasher, Hasher};

// #[derive(Debug)]
// pub struct IdentityHasher {
//     hash: u64,
// }

// impl IdentityHasher {
//     fn new() -> Self {
//         IdentityHasher { hash: 0 }
//     }
// }

// impl Hasher for IdentityHasher {
//     fn write(&mut self, bytes: &[u8]) {
//         if let Ok(string) = std::str::from_utf8(bytes) {
//             if let Ok(num) = string.parse::<u64>() {
//                 self.hash = num;
//                 return;
//             }
//         }

//         self.hash = u64::from_ne_bytes(bytes[0..8].try_into().unwrap_or([0; 8]));
//     }

//     fn finish(&self) -> u64 {
//         self.hash
//     }
// }

// impl Default for IdentityHasher {
//     fn default() -> Self {
//         IdentityHasher::new()
//     }
// }

// struct IdentityHasherBuilder;

// impl BuildHasher for IdentityHasherBuilder {
//     type Hasher = IdentityHasher;

//     fn build_hasher(&self) -> Self::Hasher {
//         IdentityHasher::new()
//     }
// }