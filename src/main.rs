
use std::{hash::DefaultHasher, time::Instant};

use my_consistent_hashing::consistent_hashing::ConsistentHashing;

fn main() {

    let mut cons = ConsistentHashing::<DefaultHasher>::new(100);

    let mut nodes = vec![];

    println!("Inserting");
    let begin = Instant::now();
    for i in 0..10_000 {

        if let Ok(item) = cons.add_node(&format!("node{}", i)) {
            for pair in item {
                nodes.push(pair);
            }
        }

    }
    println!("Done in {:?}", begin.elapsed());

    let begin = Instant::now();
    let _ = cons.remove_node("node100");
    println!("Done in {:?}", begin.elapsed());

}