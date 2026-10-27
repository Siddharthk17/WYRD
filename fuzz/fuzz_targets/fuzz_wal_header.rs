#![no_main]

use anvil_storage::{SectorSize, WalSegmentHeader};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    for sector_size in [SectorSize::Sector512, SectorSize::Sector4096] {
        if let Ok(hdr) = WalSegmentHeader::decode(data, sector_size) {
            assert_eq!(
                hdr.header_crc,
                hdr.compute_crc(),
                "decoded WalSegmentHeader has invalid CRC"
            );
            assert_eq!(
                hdr.sector_size, sector_size,
                "decoded sector size mismatch"
            );

            let mut out = vec![0u8; sector_size.as_usize()];
            if hdr.encode(&mut out).is_ok() {
                let re_decoded = WalSegmentHeader::decode(&out, sector_size)
                    .expect("re-decoding encoded valid header must succeed");
                assert_eq!(hdr, re_decoded, "roundtrip header mismatch");
            }
        }
    }
});
