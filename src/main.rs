mod chunking;
use chunking::{ae::*, chunker::*, fastcdc::*, fixed::*, rabin::*};
use sp_core::{Blake2Hasher, Hasher, H256};
use std::collections::HashSet;

use chrono::prelude::*;
extern crate num_cpus;

use std::fs::File;
use std::io::{BufReader, Cursor, Read};

fn test_chunking(chunker: &mut dyn Chunker, data: &mut dyn Read) -> (usize, usize, usize) {
    let mut chunks: HashSet<H256> = HashSet::new();
    let mut chunk = vec![];

    let mut economed = 0usize;
    let mut all = 0usize;
    while chunker.next_chunk(data, &mut chunk).unwrap() == ChunkerStatus::Working {
        let hash = Blake2Hasher::hash(&chunk);
        if chunks.contains(&hash) {
            economed += chunk.len();
        } else {
            chunks.insert(hash);
        }
        all += chunk.len();
        chunk = vec![];
    }
    let hash = Blake2Hasher::hash(&chunk);
    if chunks.contains(&hash) {
        economed += chunk.len();
    } else {
        chunks.insert(hash);
    }
    all += chunk.len();

    (economed, all, chunks.len())
}

fn main() -> std::io::Result<()> {
    let n = 16usize;
    let f = File::open("./src/emails.csv")?;
    let mut reader = BufReader::new(&f);
    let mut chunker = AeChunker::new(1024 * n);

    let start = Utc::now().timestamp_millis();
    let (economed, all, cnt) = test_chunking(&mut chunker, &mut reader);
    println!(
        "economed = {}; total = {}; count = {}; time = {}",
        economed,
        all,
        cnt,
        ((Utc::now().timestamp_millis() - start) as f64) / 1000.0
    );

    Ok(())
}
