use super::super::chunker::Chunker;
use super::ae::ChunkerAE;

#[test]
fn test_random_data() {
    let data: Vec<u8> = vec![43, 11, 5, 107, 14, 131, 98, 12, 139, 250, 23, 134, 32, 11];
    let avg_size: usize = 3;
    let result = (ChunkerAE::new(avg_size)).chunk(&data);

    assert_eq!(result, vec![2, 7, 11, 13, 14]);
}

#[test]
fn test_empty_data() {
    assert_eq!(
        (ChunkerAE::new(15)).chunk(&Vec::new()),
        vec![] as Vec<usize>
    );
}

#[test]
fn test_pseudoreal_data() {
    let data: Vec<u8> = vec![11, 7, 4, 5, 11, 15, 3, 8, 7, 4, 5, 11, 7, 4, 5, 11];
    let avg_size: usize = 7;
    let result = (ChunkerAE::new(avg_size)).chunk(&data);

    assert_eq!(result, vec![4, 9, 15, 16]);
}
