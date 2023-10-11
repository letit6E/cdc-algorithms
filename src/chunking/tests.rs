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