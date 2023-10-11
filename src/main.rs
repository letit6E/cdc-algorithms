mod chunking;
use chunking::{ae::*, chunker::*, fastcdc::*, rabin::*};
use fastcdc::v2020::Chunk;
use rayon::prelude::*;

use std::collections::hash_map::DefaultHasher;
use std::collections::HashSet;
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::io::{BufReader, Cursor, Error, Read, Seek, SeekFrom};
use std::thread;
use std::time::{Duration, Instant};

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

fn internal_chunking(path: &str, n: usize) -> Vec<Chunk> {
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

fn fcdc_test(path: &str, n: usize) -> Vec<Vec<u8>> {
    let f = File::open(path).unwrap();
    let mut reader = BufReader::new(&f);
    let mut chunker = FastCdcChunker::new(1024 * n, 1);

    let result = test_chunking(&mut chunker, &mut reader);
    result
}

fn ae_test(path: &str, n: usize) -> Vec<Vec<u8>> {
    let f = File::open(path).unwrap();
    let mut reader = BufReader::new(&f);
    let mut chunker = AeChunker::new(1024 * n);

    let result = test_chunking(&mut chunker, &mut reader);
    result
}

fn rabin_test(path: &str, n: usize) -> Vec<Vec<u8>> {
    let f = File::open(path).unwrap();
    let mut reader = BufReader::new(&f);
    let mut chunker = RabinChunker::new(1024 * n, 0);

    let result = test_chunking(&mut chunker, &mut reader);
    result
}

fn read_file(path: &str) -> Result<Vec<u8>, Error> {
    let mut f = File::open(path)?;
    let mut buffer = Vec::new();
    f.read_to_end(&mut buffer)?;

    Ok(buffer)
}

fn hash_vec<T: Hash>(v: &Vec<T>) -> u64 {
    let mut hasher = DefaultHasher::new();
    v.hash(&mut hasher);
    hasher.finish()
}

fn preprocess_vec(buffer: &Vec<u8>, threads_cnt: usize) -> Vec<Vec<u8>> {
    let mut vecs: Vec<Vec<u8>> = vec![Vec::new(); threads_cnt];
    for i in 0..threads_cnt {
        let left = i * buffer.len() / threads_cnt;
        let right = (i + 1) * buffer.len() / threads_cnt;
        vecs[i] = (buffer[left..right]).to_vec();
    }
    return vecs;
}

fn work_for_thread(i: usize, n: usize, cur: &Vec<u8>) -> (HashSet<u64>, usize, Vec<u8>) {
    let mut chunker = AeChunker::new(1024 * n);
    // let mut chunker = FastCdcChunker::new(1024 * n, 1);
    // let mut chunker = RabinChunker::new(1024 * n, 0);
    let mut cursor = Cursor::new(cur);
    let mut set = HashSet::new();
    let mut result = 0;

    let vecs = test_chunking(&mut chunker, &mut cursor);
    let left = if i % 2 == 0 { 0 } else { 1 };
    let right = if i % 2 == 0 {
        vecs.len() - 1
    } else {
        vecs.len()
    };

    for i in left..right {
        let hsh = hash_vec(&vecs[i]);
        if !set.contains(&hsh) {
            set.insert(hsh);
            result += vecs[i].len();
        }
    }

    let remainder = if i % 2 == 0 {
        vecs.last()
    } else {
        vecs.first()
    };
    (set, result, remainder.unwrap().clone())
}

fn collect_results(
    result_set: &mut HashSet<u64>,
    mut ans: usize,
    remainders: &mut Vec<Vec<u8>>,
    n: usize,
) -> (usize, usize) {
    let mut i = 0;
    while i < remainders.len() - 1 {
        let mut t = remainders[i + 1].clone();
        remainders[i].append(&mut t);
        let mut chunker = AeChunker::new(1024 * n);
        // let mut chunker = FastCdcChunker::new(1024 * n, 1);
        // let mut chunker = RabinChunker::new(1024 * n, 0);
        let mut cursor = Cursor::new(&remainders[i]);
        for elem in test_chunking(&mut chunker, &mut cursor) {
            let hsh = hash_vec(&elem);
            if !result_set.contains(&hsh) {
                result_set.insert(hsh);
                ans += elem.len();
            }
        }
        i += 2;
    }

    (ans, result_set.len())
}

fn threads_test(path: &str, n: usize, threads_cnt: usize) -> (Duration, f64, f64) {
    let buffer = read_file(path).unwrap();
    let vecs = preprocess_vec(&buffer, threads_cnt);

    let now = Instant::now();
    let (len, cnt) = {
        let mut threads = vec![];

        for i in 0..threads_cnt {
            let cur = vecs[i].clone();
            threads.push(thread::spawn(move || work_for_thread(i, n, &cur)));
        }

        let mut result_set: HashSet<u64> = HashSet::new();
        let mut ans = 0;
        let mut remainders = vec![];
        for handle in threads {
            let (st, x, y) = handle.join().unwrap();
            result_set.extend(&st);
            remainders.push(y);
            ans += x;
        }

        collect_results(&mut result_set, ans, &mut remainders, n)
    };

    let elapsed = now.elapsed();
    return (
        elapsed,
        (len as f64) / (buffer.len() as f64),
        (len as f64) / (cnt as f64),
    );
}

fn rayon_test(path: &str, n: usize, threads_cnt: usize) -> (Duration, f64, f64) {
    let buffer = read_file(path).unwrap();
    let mut vecs = preprocess_vec(&buffer, threads_cnt);

    let now = Instant::now();
    let (len, cnt) = {
        let tmp: Vec<(HashSet<u64>, usize, Vec<u8>)> = (0..threads_cnt)
            .into_par_iter()
            .map(|i| {
                let cur = vecs[i].clone();
                work_for_thread(i, n, &cur)
            })
            .collect();

        let mut result_set: HashSet<u64> = HashSet::new();
        let mut ans = 0;
        let mut remainders = vec![];
        for handle in tmp {
            let (st, x, y) = handle;
            result_set.extend(&st);
            remainders.push(y);
            ans += x;
        }

        collect_results(&mut result_set, ans, &mut remainders, n)
    };

    let elapsed = now.elapsed();
    return (
        elapsed,
        (len as f64) / (buffer.len() as f64),
        (len as f64) / (cnt as f64),
    );
}

fn main() {
    let path = "./resources/valvesockets.tar";
    let n = 8usize;
    let threads_cnt = 3usize;
}
