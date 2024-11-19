use std::{collections::{BTreeMap, HashSet}, fmt, hash::{Hash, Hasher}};

use crate::transaction::Transaction;

pub struct ConsistentHashing<T: Hasher + Default> {
    ring: BTreeMap<u64, String>,
    pub nodes: HashSet<String>,
    virtual_nodes_count: u32,
    _hasher: T,
}

#[derive(Debug)]
pub enum ConsistentHashingError {
    NodeAlreadyExists(String),
    NodeDoesNotExist(String),
    RingIsEmpty(String),
    ZeroVirtualNodes(String),
    UnchangedVirtualNodeCount(String)
}

// Implement the `std::fmt::Display` trait for user-friendly error messages
// impl fmt::Display for ConsistentHashingError {
//     fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
//         match self {
//             ConsistentHashingError::NodeAlreadyExists(msg) => write!(f, "Connection error: {}", msg),
//             ConsistentHashingError::QueryError(msg) => write!(f, "Query error: {}", msg),
//             ConsistentHashingError::NotFound(msg) => write!(f, "Not found: {}", msg),
//         }
//     }
// }

// impl std::error::Error for ConsistentHashingError {}

impl<T: Hasher + Default> ConsistentHashing<T> {
    
    pub fn new(virtual_nodes_count: u32) -> Self {
        return ConsistentHashing {
            ring: BTreeMap::new(),
            nodes: HashSet::new(),
            virtual_nodes_count,
            _hasher: T::default(),
        };
    }

    pub fn new_with_nodes(virtual_nodes_count: u32, nodes: Vec<String>) -> Self {
        let mut consistent_hashing = ConsistentHashing::new(virtual_nodes_count);
        for node in nodes {
            match consistent_hashing.add_node(&node) {
                Ok(_) => (),
                Err(_) => panic!("Node already exists")
            };
        }
        return consistent_hashing;
    }

    fn get_virtual_node_form(&self, node: &str, i: u32) -> String {
        return format!("{}-{}", node, i);
    }

    fn get_current_state(&self) -> Vec<(u64, String)> {
        let mut x: Vec<(u64, String)> = self.ring.iter().map(|(k, v)| (*k, v.clone())).collect();
        x.sort_by(|a, b| a.0.cmp(&b.0));
        return x;
    }

    fn hash<U: Hash>(&self, item: &U) -> u64 {
        let mut hasher = T::default();
        item.hash(&mut hasher);
        return hasher.finish();
    }

    fn get_previous_node(&self, node: &str) -> Option<(&u64, &String)> {
        
        let hashed_value = self.hash(&node.to_string());
        if let Some(prev) = self.ring.range(..hashed_value).next_back() {
            return Some(prev);
        }
        return self.ring.iter().next_back().clone();
    }

    fn get_previous_node_by_hash(&self, hash: u64) -> Option<(&u64, &String)> {
        if let Some(prev) = self.ring.range(..hash).next_back() {
            return Some(prev);
        }
        return self.ring.iter().next_back().clone();
    }

    fn get_next_node(&self, node: &str) -> Option<(&u64, &String)> {
        let hashed_value = self.hash(&node.to_string());
        if let Some(prev) = self.ring.range(hashed_value..).skip(1).next() {
            return Some(prev);
        }
        return self.ring.iter().next().clone();
    }

    fn get_next_node_by_hash(&self, hash: u64) -> Option<(&u64, &String)> {
        if let Some(prev) = self.ring.range(hash..).skip(1).next() {
            return Some(prev);
        }
        return self.ring.iter().next().clone();
    }

    /// hashes nodex-i ...
    pub fn add_node(&mut self, node: &str) -> Result<(Vec<(String, u64)>, Vec<Transaction>), ConsistentHashingError> {

        if self.nodes.contains(node) {
            return Err(ConsistentHashingError::NodeAlreadyExists("This node already exist".to_string()));
        }

        let mut hashes = vec![];
        let mut transactions = vec![];
        self.nodes.insert(node.to_string());

        for i in 0..self.virtual_nodes_count {
            let v_node = self.get_virtual_node_form(node, i);
            let hash = self.hash(&v_node);
            self.ring.insert(hash, node.to_string());
            hashes.push((format!("{}-{}", node, i), hash));
        }

        let mut seen_v_node = HashSet::new();

        for i in 0..self.virtual_nodes_count {
            
            if self.nodes.len() < 2 {
                break;
            }

            let v_node = self.get_virtual_node_form(node, i);
            let hash = self.hash(&v_node);

            if seen_v_node.contains(&hash) {
                continue;
            }

            seen_v_node.insert(hash);

            let mut prev_node = self.get_previous_node(&v_node).expect("This should never fail. If it failed, check condition for nodes.len() > 2");
            let mut next_node = self.get_next_node(&v_node).expect("This should never fail. If it failed, check condition for nodes.len() > 2");

            while prev_node.1 == node {
                let new_hash = *prev_node.0;
                seen_v_node.insert(new_hash);
                prev_node = self.get_previous_node_by_hash(new_hash).unwrap();
            }

            if next_node.1 == node {
                let new_hash = *next_node.0;
                seen_v_node.insert(new_hash);
                next_node = self.get_next_node_by_hash(new_hash).unwrap();
            }

            let new_hash = *next_node.0;
            let final_virtual_node = self.get_previous_node_by_hash(new_hash).unwrap();

            let new_transaction = Transaction::new(
                next_node.1.to_string(),
                node.to_string(),
                *prev_node.0, 
                *final_virtual_node.0
            );
            transactions.push(new_transaction);

        }

        return Ok((hashes, transactions));
    }

    pub fn remove_node(&mut self, node: &str) -> Result<Vec<Transaction>, ConsistentHashingError> {

        if !self.nodes.contains(node) {
            return Err(ConsistentHashingError::NodeDoesNotExist("This node doesn't exist".to_string()));
        }

        let mut seen_v_node = HashSet::new();
        let mut transactions = vec![];
        self.nodes.remove(node);

        for i in 0..self.virtual_nodes_count {
            
            let v_node = self.get_virtual_node_form(node, i);
            let hash = self.hash(&v_node);

            if seen_v_node.contains(&hash) {
                continue;
            }

            seen_v_node.insert(hash);

            let mut prev_node = self.get_previous_node(&v_node).expect("This should never fail. If it failed, check condition for nodes.len() > 2");
            let mut next_node = self.get_next_node(&v_node).expect("This should never fail. If it failed, check condition for nodes.len() > 2");

            while prev_node.1 == node {
                let new_hash = *prev_node.0;
                seen_v_node.insert(new_hash);
                prev_node = self.get_previous_node_by_hash(new_hash).unwrap();
            }

            if next_node.1 == node {
                let new_hash = *next_node.0;
                seen_v_node.insert(new_hash);
                next_node = self.get_next_node_by_hash(new_hash).unwrap();
            }

            let new_hash = *next_node.0;
            let final_virtual_node = self.get_previous_node_by_hash(new_hash).unwrap();

            let new_transaction = Transaction::new(
                node.to_string(),
                next_node.1.to_string(),
                *prev_node.0,
                *final_virtual_node.0
            );

            transactions.push(new_transaction);

        }

        for i in 0..self.virtual_nodes_count {
            let v_node = self.get_virtual_node_form(node, i);
            let hash = self.hash(&v_node);
            self.ring.remove(&hash);
        }
        return Ok(transactions);
    }

    pub fn set_virtual_nodes_count(&mut self, count: u32) -> Result<Vec<Transaction>, ConsistentHashingError> {
        
        if count == 0 {
            return Err(ConsistentHashingError::ZeroVirtualNodes("Cannot set virtual nodes count to 0".to_string()));
        }

        if count == self.virtual_nodes_count {
            return Err(ConsistentHashingError::UnchangedVirtualNodeCount("New virtual nodes count is same as current".to_string()));
        }

        let mut transactions = vec![];
        let diff: i32 = count as i32 - self.virtual_nodes_count as i32;

        if diff > 0 {
            // add nodes
            for node in &self.nodes {
                for i in self.virtual_nodes_count..count {

                    let v_node = self.get_virtual_node_form(node, i);
                    println!("adding v_node: {}", v_node);
                    let hash = self.hash(&v_node);

                    self.ring.insert(hash, node.to_string());
                    let prev = self.get_previous_node_by_hash(hash).unwrap();
                    let next = self.get_next_node_by_hash(hash).unwrap();

                    if next.1 != node {
                        let transaction = Transaction::new(
                            next.1.to_string(),
                            node.to_string(),
                            *prev.0,
                            hash
                        );
                        println!("{} with hash: {}", v_node, hash);
                        println!("trans {:?}", transaction);
                        transactions.push(transaction);
                    }


                    let x = self.get_current_state();
                    for pair in x {
                        println!("{}: {}", pair.1, pair.0);
                    }
                }
            }
        }
        else {
            // remove nodes
            for node in &self.nodes {
                for i in (count..self.virtual_nodes_count).rev() {
                    
                    let x = self.get_current_state();
                    for pair in x {
                        println!("{}: {}", pair.1, pair.0);
                    }
                    
                    let v_node = self.get_virtual_node_form(node, i);
                    let hash = self.hash(&v_node);

                    let prev = self.get_previous_node_by_hash(hash).unwrap();
                    let next = self.get_next_node_by_hash(hash).unwrap();

                    if next.1 != node {
                        let transaction = Transaction::new(
                            node.to_string(), 
                            next.1.to_string(),
                            *prev.0,
                            hash
                        );
                        println!("{} with hash: {}", v_node, hash);
                        println!("trans {:?}", transaction);
                        transactions.push(transaction);
                    }

                    self.ring.remove(&hash);
                }
            }
        }

        self.virtual_nodes_count = count;
        return Ok(transactions);
    }

    pub fn get_node<U: Hash>(&self, key: &U) -> Option<&String> {
        if self.ring.is_empty() {
            return None;
        }
        let hash = self.hash(key);
        println!("key hash: {}", hash);
        let node = self.ring
            .range(hash..)
            .next()
            .or_else(|| self.ring.iter().next());
        return Some(node.unwrap().1);
            
    }

}
