use super::ae::ChunkerAE;
use super::chunker::Chunker;

#[test]
fn test_random_data() {
    let data: Vec<u8> = vec![
        64, 229, 155, 85, 19, 148, 34, 203, 75, 89, 140, 164, 236, 47, 104, 56, 29, 9, 187, 69,
        232, 24, 139, 162, 18, 254, 185, 42, 80, 61, 152, 132, 135, 239, 39, 14, 249, 132, 222, 83,
        18, 101, 52, 49, 202, 171, 182, 217, 49, 12, 75, 102, 17, 111, 86, 25, 156, 106, 136, 23,
        134, 129, 121, 3, 46, 16, 169, 194, 93, 99, 114, 67, 198, 180, 94, 77, 7, 135, 210, 177, 2,
        69, 150, 77, 233, 13, 74, 149, 191, 91, 32, 110, 28, 89, 80, 177, 232, 18, 106, 91,
    ];
    let avg_size: usize = 9;
    let result = (ChunkerAE::new(avg_size)).chunk(&data);

    assert_eq!(result, vec![7, 18, 31, 42, 53, 62, 78, 84, 90, 100]);
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

    assert_eq!(result, vec![5, 10, 16]);
}
