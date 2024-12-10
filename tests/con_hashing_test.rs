#[cfg(test)]
mod tests {

    use consistent_hashing_aa::consistent_hashing::ConsistentHashing;

     // Import functions from the outer module

    #[test]
    fn test_add_node_1() {

        let mut cons = ConsistentHashing::new(1);

        for i in 0..5 {
            let node = format!("node{}", i);
            cons.add_node(&node).unwrap();
        }

        let state = cons.get_current_state();

        assert_eq!(state.len(), 5);

    }

}