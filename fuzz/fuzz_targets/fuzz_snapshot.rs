#![no_main]

use anvil_storage::{SnapshotChunk, SnapshotFooter, SnapshotHeader};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    // 1. Snapshot Header
    if let Ok(hdr) = SnapshotHeader::decode(data) {
        assert_eq!(
            hdr.compute_crc(),
            hdr.header_crc,
            "decoded snapshot header has invalid CRC"
        );
        let mut out = [0u8; 64];
        if hdr.encode(&mut out).is_ok() {
            let re_decoded = SnapshotHeader::decode(&out)
                .expect("re-decoding encoded valid snapshot header must succeed");
            assert_eq!(hdr, re_decoded);
        }
    }

    // 2. Snapshot Chunk
    if let Ok((chunk, consumed)) = SnapshotChunk::decode(data, 0) {
        assert!(consumed <= data.len(), "consumed {consumed} > len {}", data.len());
        let mut out = Vec::new();
        chunk.encode(&mut out);
        let (re_chunk, re_consumed) = SnapshotChunk::decode(&out, 0)
            .expect("re-decoding encoded valid snapshot chunk must succeed");
        assert_eq!(chunk, re_chunk);
        assert_eq!(out.len(), re_consumed);
    }

    // 3. Snapshot Footer
    if let Ok(footer) = SnapshotFooter::decode(data) {
        assert_eq!(
            footer.compute_crc(),
            footer.footer_crc,
            "decoded snapshot footer has invalid CRC"
        );
        let mut out = [0u8; 64];
        if footer.encode(&mut out).is_ok() {
            let re_decoded = SnapshotFooter::decode(&out)
                .expect("re-decoding encoded valid snapshot footer must succeed");
            assert_eq!(footer, re_decoded);
        }
    }
});
