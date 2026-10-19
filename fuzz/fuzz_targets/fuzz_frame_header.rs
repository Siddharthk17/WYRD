#![no_main]

use anvil_types::FrameHeader;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let mut cursor = data;
    let _ = FrameHeader::decode(&mut cursor);
});
