#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use anvil_types::{
    CODEC_VERSION_1, Entry, FrameHeader, FrameType, LogIndex, Term, encode_varint_u64,
};
use bytes::Bytes;
use std::fs;
use std::path::Path;

#[test]
fn test_generate_and_verify_seed_corpus() {
    let base = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fuzz/corpus");

    let varint_dir = base.join("fuzz_varint");
    let header_dir = base.join("fuzz_frame_header");
    let entry_dir = base.join("fuzz_entry");

    fs::create_dir_all(&varint_dir).expect("create varint corpus dir");
    fs::create_dir_all(&header_dir).expect("create header corpus dir");
    fs::create_dir_all(&entry_dir).expect("create entry corpus dir");

    // Varint seeds
    for (i, val) in [0u64, 1, 127, 128, 16384, u64::MAX].iter().enumerate() {
        let mut buf = Vec::new();
        encode_varint_u64(*val, &mut buf);
        fs::write(varint_dir.join(format!("seed_{i}")), &buf).expect("write varint seed");
    }

    // FrameHeader seeds
    let header = FrameHeader {
        version: CODEC_VERSION_1,
        frame_type: FrameType::Entry.as_u16(),
        payload_len: 0,
        checksum: 0x1234_5678,
    };
    let mut header_buf = Vec::new();
    header.encode(&mut header_buf);
    fs::write(header_dir.join("seed_valid_header"), &header_buf).expect("write header seed");

    // Entry seeds
    let entry_noop = Entry::noop(LogIndex::from_raw(1), Term::from_raw(1));
    let mut noop_buf = Vec::new();
    entry_noop.encode(&mut noop_buf);
    fs::write(entry_dir.join("seed_noop"), &noop_buf).expect("write noop seed");

    let entry_normal = Entry::normal(
        LogIndex::from_raw(2),
        Term::from_raw(1),
        Bytes::from_static(b"seed payload command"),
    );
    let mut normal_buf = Vec::new();
    entry_normal.encode(&mut normal_buf);
    fs::write(entry_dir.join("seed_normal"), &normal_buf).expect("write normal seed");
}
