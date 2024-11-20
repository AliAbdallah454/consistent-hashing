
use std::{hash::DefaultHasher, time::Instant};

use my_consistent_hashing::consistent_hashing::ConsistentHashing;

fn main() {

    let mut cons = ConsistentHashing::<DefaultHasher>::new(2);

    // let mut nodes = vec![];

    // println!("Inserting");
    let begin = Instant::now();
    for i in 0..6 {

        if let Ok(item) = cons.add_node(&format!("node{}", i)) {
            // for tran in item {
            //     println!("Trans : {:?}", tran);
            // }
        }
        cons.get_current_state();
    }
    // println!("Done in {:?}", begin.elapsed());

    // let begin = Instant::now();
    // let x = cons.remove_node("node4").unwrap();
    // for t in x {
    //     println!("Trans : {:?}", t);
    // }
    // cons.get_current_state();
    // println!("Done in {:?}", begin.elapsed());
    println!("Changing v_node_count");
    cons.set_virtual_nodes_count(3);

}