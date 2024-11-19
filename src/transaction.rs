#[derive(Debug, Clone)]
pub struct Transaction {
    source: String,
    destination: String,
    begining: u64,
    ending: u64,
}

impl Transaction {
    pub fn new(source: String, destination: String, begining: u64, ending: u64) -> Self {
        return Transaction {
            source,
            destination,
            begining,
            ending,
        };
    }
}