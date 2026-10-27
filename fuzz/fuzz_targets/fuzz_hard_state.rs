#![no_main]

use anvil_storage::{HardStateSlot, SectorSize, recover_hard_state};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    for sector_size in [SectorSize::Sector512, SectorSize::Sector4096] {
        for slot_idx in [0u16, 1u16] {
            if let Ok(slot) = HardStateSlot::decode(data, slot_idx, sector_size) {
                assert_eq!(
                    slot.compute_crc(),
                    slot.crc32c,
                    "decoded slot has invalid CRC"
                );
                assert_eq!(slot.slot_index, slot_idx);

                let mut out = vec![0u8; sector_size.as_usize()];
                if slot.encode(sector_size, &mut out).is_ok() {
                    let re_decoded = HardStateSlot::decode(&out, slot_idx, sector_size)
                        .expect("re-decoding encoded valid slot must succeed");
                    assert_eq!(slot, re_decoded);
                }
            }
        }

        let _ = recover_hard_state(data, sector_size);
    }
});
