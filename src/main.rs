
use std::{hash::DefaultHasher, time::Instant};

use my_consistent_hashing::consistent_hashing::ConsistentHashing;

fn main() {

    let mut cons = ConsistentHashing::<DefaultHasher>::new(2);

    let mut nodes = vec![];

    println!("Inserting");
    let begin = Instant::now();
    for i in 0..6 {

        if let Ok((item, _)) = cons.add_node(&format!("node{}", i)) {
            for pair in item {
                nodes.push(pair);
            }
        }

    }
    println!("Done in {:?}", begin.elapsed());

    let trans = cons.set_virtual_nodes_count(3).unwrap();
    for t in trans {
        println!("{:?}", t);
    }

}