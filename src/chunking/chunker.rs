use quick_error::quick_error;
use std::io::{self, Read, Write};
#[derive(PartialEq)]
pub enum ChunkerStatus {
    Working,
    Finished,
}

quick_error! {
    #[derive(Debug)]
    pub enum ChunkerError {
        Read(err: io::Error) {
            display("Error while reading: {err}")
        }
        Write(err: io::Error) {
            display("Error while writing: {err}")
        }
        New(err: &'static str) {
            display("Chunker error: {err}")
        }
    }
}

pub trait Chunker {
    fn next_chunk(
        &mut self,
        input: &mut dyn Read,
        output: &mut dyn Write,
    ) -> Result<ChunkerStatus, ChunkerError>;
}
