use std::time::Instant;
use my_consistent_hashing::consistent_hashing::ConsistentHashing;

fn main() {

    let mut cons = ConsistentHashing::new(2);

    let begin = Instant::now();
    for i in 0..10 {
        cons.add_node(&format!("node{}", i)).unwrap();
    }
    println!("Done in {:?}", begin.elapsed());
    let (node, key_hash) = cons.get_node(&"gay".to_string());

    println!("node: {}", node.unwrap());
    println!("hash: {}", key_hash.unwrap());

}