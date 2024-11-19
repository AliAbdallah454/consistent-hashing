use std::{collections::{BTreeMap, HashSet}, hash::{DefaultHasher, Hash, Hasher}};

use my_consistent_hashing::transaction::{self, Transaction};

struct ConsistentHashing<T: Hasher + Default> {
    ring: BTreeMap<u64, String>,
    nodes: HashSet<String>,
    virtual_nodes_count: u32,
    _hasher: T,
}

impl<T: Hasher + Default> ConsistentHashing<T> {
    
    fn new(virtual_nodes_count: u32) -> Self {
        return ConsistentHashing {
            ring: BTreeMap::new(),
            nodes: HashSet::new(),
            virtual_nodes_count,
            _hasher: T::default(),
        };
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
    fn add_node(&mut self, node: &str) -> (Vec<(String, u64)>, Vec<Transaction>) {

        let mut hashes = vec![];
        let mut transactions = vec![];

        self.nodes.insert(node.to_string());
        for i in 0..self.virtual_nodes_count {
            let v_node = self.get_virtual_node_form(node, i);
            let hash = self.hash(&v_node);
            println!("{} -> {}", v_node, hash);
            self.ring.insert(hash, node.to_string());
            hashes.push((format!("{}-{}", node, i), hash));
        }

        let mut seen_v_node = HashSet::new();

        for i in 0..self.virtual_nodes_count {
            let v_node = self.get_virtual_node_form(node, i);

            let hash = self.hash(&v_node);

            if seen_v_node.contains(&hash) {
                continue;
            }

            seen_v_node.insert(hash);

            let mut prev_node = match self.get_previous_node(&v_node) {
                Some(node) => node,
                _ => (&0, &"".to_string()),
            };
            let mut next_node = match self.get_next_node(&v_node) {
                Some(node) => node,
                None => (&0, &"".to_string()),
            };

            if self.nodes.len() < 2 {
                continue;
            }

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

            println!("keys in range {} -> {} should be moved from {} -> {}", prev_node.0, final_virtual_node.0, next_node.1, node);
        }

        return (hashes, transactions);
    }

    fn remove_node(&mut self, node: &str) -> Vec<Transaction> {

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

            let mut prev_node = match self.get_previous_node_by_hash(hash) {
                Some(node) => node,
                _ => {
                    println!("ERRRRR");
                    (&0, &"".to_string())
                }
            };
            
            let mut next_node = match self.get_next_node_by_hash(hash) {
                Some(node) => node,
                _ => {
                    println!("ERRRRR");
                    (&0, &"".to_string())
                }
            };

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
            println!("keys in range {} -> {} should be moved from {} -> {}", prev_node.0, final_virtual_node.0, node, next_node.1);

        }

        for i in 0..self.virtual_nodes_count {
            let v_node = self.get_virtual_node_form(node, i);
            let hash = self.hash(&v_node);
            self.ring.remove(&hash);
        }
        return transactions;
    }

    fn get_node<U: Hash>(&self, key: &U) -> Option<&String> {
        let hash = self.hash(key);
        println!("key hash: {}", hash);
        let node = self.ring
            .range(hash..)
            .next()
            .or_else(|| self.ring.iter().next());
        return Some(node.unwrap().1);
            
    }

}

fn main() {

    let mut map = BTreeMap::new();

    map.insert(10, 4);
    map.insert(20, 4);
    map.insert(1, 4);
    map.insert(9, 4);
    map.insert(2, 4);

    println!("{:?}", map.iter());

    // let mut cons = ConsistentHashing::<DefaultHasher>::new(2);

    // let mut nodes = vec![];

    // for i in 0..6 {
    //     for pair in cons.add_node(&format!("node{}", i)).0 {
    //         nodes.push(pair);
    //     }
    // }

    // nodes.sort_by(|a, b| a.1.cmp(&b.1));
    // for x in &nodes {
    //     println!("{} - {}", x.0, x.1);
    // }

    // let trans = cons.remove_node("node3");

    // for t in trans {
    //     println!("{:?}", t);
    // }
    // println!("{:?}", cons.get_node(&"keyxy".to_string()));


    // let (_, trans) = cons.add_node("node10");
    
    // for t in trans {
    //     println!("{:?}", t);
    // }
    // println!("{:?}", cons.nodes);

}