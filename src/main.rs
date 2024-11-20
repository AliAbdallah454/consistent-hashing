
use std::{hash::DefaultHasher, time::Instant};

use my_consistent_hashing::consistent_hashing::ConsistentHashing;

fn main() {

    let mut cons = ConsistentHashing::<DefaultHasher>::new(100+1);

    let begin = Instant::now();
    for i in 0..10_000 {
        cons.add_node(&format!("node{}", i));
        // if let Ok(item) = cons.add_node(&format!("node{}", i)) {
            // for tran in item {
            //     println!("Trans : {:?}", tran);
            // }
        // }
        // cons.get_current_state();
    }
    println!("Done in {:?}", begin.elapsed());

}