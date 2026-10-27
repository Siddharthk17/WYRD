#![no_main]

use anvil_storage::{
    SectorSize, WalScanResult, compute_wal_record_crc, decode_wal_frame,
};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    for sector_size in [SectorSize::Sector512, SectorSize::Sector4096] {
        let segment_id = 1;
        let file_offset = sector_size.as_u64();
        let prev_crc = 0;

        let res = decode_wal_frame(data, sector_size, prev_crc, segment_id, file_offset);
        if let WalScanResult::Valid {
            frame,
            bytes_consumed,
            next_crc,
        } = res
        {
            assert!(
                bytes_consumed <= data.len(),
                "consumed {bytes_consumed} > data len {}",
                data.len()
            );
            assert_eq!(
                next_crc, frame.crc32c,
                "next_crc must match frame.crc32c"
            );

            let computed_crc = compute_wal_record_crc(
                prev_crc,
                segment_id,
                file_offset,
                frame.index,
                frame.term,
                frame.kind,
                &frame.payload,
            );
            assert_eq!(
                frame.crc32c, computed_crc,
                "decoded frame has invalid CRC"
            );

            let mut encoded = Vec::new();
            frame.encode_frame(sector_size, &mut encoded);
            let re_res = decode_wal_frame(&encoded, sector_size, prev_crc, segment_id, file_offset);
            match re_res {
                WalScanResult::Valid {
                    frame: re_frame,
                    bytes_consumed: re_consumed,
                    ..
                } => {
                    assert_eq!(frame, re_frame);
                    assert_eq!(encoded.len(), re_consumed);
                }
                other => panic!("re-decoding encoded valid frame failed: {other:?}"),
            }
        }
    }
});
