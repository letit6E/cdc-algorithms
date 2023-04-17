pub trait Chunker {
    fn next_chunk(&mut self, data: &Vec<u8>, start: usize) -> usize;
    fn chunk(&mut self, data: &Vec<u8>) -> Vec<usize>;
}
