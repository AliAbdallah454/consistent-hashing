use std::{collections::{BTreeMap, HashSet}, hash::{Hash, Hasher}};

use crate::transaction::Transaction;

pub struct ConsistentHashing<T: Hasher + Default> {
    ring: BTreeMap<u64, String>,
    pub nodes: HashSet<String>,
    virtual_nodes_count: u32,
    _hasher: T,
}

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
            consistent_hashing.add_node(&node);
        }
        return consistent_hashing;
    }

    fn get_virtual_node_form(&self, node: &str, i: u32) -> String {
        return format!("{}-{}", i, node);
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
    pub fn add_node(&mut self, node: &str) -> (Vec<(String, u64)>, Vec<Transaction>) {

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

        return (hashes, transactions);
    }

    pub fn remove_node(&mut self, node: &str) -> Vec<Transaction> {

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
        return transactions;
    }

    pub fn set_virtual_nodes_count(&mut self, count: u32) -> Vec<Transaction> {
        
        let mut transactions = vec![];

        if count == self.virtual_nodes_count {
            return vec![];
        }

        let diff: i32 = count as i32 - self.virtual_nodes_count as i32;

        if diff > 0 {
            // add nodes
            for node in &self.nodes {
                for i in self.virtual_nodes_count..count {
                    let v_node = self.get_virtual_node_form(node, i);
                    let hash = self.hash(&v_node);
                    self.ring.insert(hash, node.to_string());
                    let prev = self.get_previous_node_by_hash(hash).unwrap();
                    let next = self.get_next_node_by_hash(hash).unwrap();

                    if next.1 == node {
                        continue;
                    }

                    let transaction = Transaction::new(
                        next.1.to_string(),
                        node.to_string(),
                        *prev.0,
                        hash
                    );
                    transactions.push(transaction);
                }
            }
        }
        else {
            // remove nodes
            for node in &self.nodes {
                for i in (count..self.virtual_nodes_count).rev() {
                    let v_node = self.get_virtual_node_form(node, i);
                    let hash = self.hash(&v_node);
                    self.ring.remove(&hash);
                    let prev = self.get_previous_node_by_hash(hash).unwrap();
                    let next = self.get_next_node_by_hash(hash).unwrap();

                    if next.1 == node {
                        continue;
                    }

                    let transaction = Transaction::new(
                        node.to_string(),
                        next.1.to_string(),
                        *prev.0,
                        hash
                    );
                    transactions.push(transaction);

                }
            }
        }
        self.virtual_nodes_count = count;
        return transactions;
    }

    pub fn get_node<U: Hash>(&self, key: &U) -> Option<&String> {
        let hash = self.hash(key);
        println!("key hash: {}", hash);
        let node = self.ring
            .range(hash..)
            .next()
            .or_else(|| self.ring.iter().next());
        return Some(node.unwrap().1);
            
    }

}
