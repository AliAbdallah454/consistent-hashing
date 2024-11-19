
use std::{hash::DefaultHasher, time::Instant};

use my_consistent_hashing::consistent_hashing::ConsistentHashing;

fn main() {

    let mut cons = ConsistentHashing::<DefaultHasher>::new(2);

    let mut nodes = vec![];

    println!("Inserting");
    let begin = Instant::now();
    for i in 0..10 {
        for pair in cons.add_node(&format!("node{}", i)).0 {
            nodes.push(pair);
        }
    }
    println!("Done in {:?}", begin.elapsed());

    nodes.sort_by(|a, b| a.1.cmp(&b.1));
    for x in &nodes {
        println!("{} - {}", x.0, x.1);
    }

    let trans = cons.set_virtual_nodes_count(1);
    for t in trans {
        println!("{:?}", t);
    }

}