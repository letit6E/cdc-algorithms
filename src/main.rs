mod chunking;
use chunking::{ae::*, chunker::*, fastcdc::*, rabin::*};
use fastcdc::v2020::Chunk;

use std::fs::File;
use std::io::{BufReader, Read};

fn test_chunking(chunker: &mut dyn Chunker, data: &mut dyn Read) -> Vec<Vec<u8>> {
    let mut chunks: Vec<Vec<u8>> = vec![];
    let mut chunk: Vec<u8> = vec![];
    while chunker.next_chunk(data, &mut chunk).unwrap() == ChunkerStatus::Working {
        chunks.push(chunk);
        chunk = vec![];
    }
    chunks.push(chunk);

    chunks
}

fn internal_chunking(n: usize, path: &str) -> Vec<Chunk> {
    let contents = std::fs::read(path).unwrap();
    let chunker = fastcdc::v2020::FastCDC::new(
        &contents,
        (n as u32) * 256,
        (n as u32) * 1024,
        (n as u32) * 8 * 1024,
    );
    let mut chunks: Vec<fastcdc::v2020::Chunk> = vec![];

    for chunk in chunker {
        chunks.push(chunk);
    }
    chunks
}

fn fcdctest(path: &str, n: usize) -> Vec<Vec<u8>> {
    let f = File::open(path).unwrap();
    let mut reader = BufReader::new(&f);
    let mut chunker = FastCdcChunker::new(1024 * n, 1);

    let result = test_chunking(&mut chunker, &mut reader);
    result
}

fn aetest(path: &str, n: usize) -> Vec<Vec<u8>> {
    let f = File::open(path).unwrap();
    let mut reader = BufReader::new(&f);
    let mut chunker = AeChunker::new(1024 * n);

    let result = test_chunking(&mut chunker, &mut reader);
    result
}

fn rabintest(path: &str, n: usize) -> Vec<Vec<u8>> {
    let f = File::open(path).unwrap();
    let mut reader = BufReader::new(&f);
    let mut chunker = RabinChunker::new(1024 * n, 0);

    let result = test_chunking(&mut chunker, &mut reader);
    result
}
fn main() {
    let path = "file";
    let n = 8usize;

    let internal_result = internal_chunking(n, path);
    let fcdc_result = fcdctest(path, n);
    let ae_result = aetest(path, n);
    let rabin_result = rabintest(path, n);
    println!(
        "{} {} {} {}",
        internal_result.len(),
        fcdc_result.len(),
        ae_result.len(),
        rabin_result.len()
    );
}
