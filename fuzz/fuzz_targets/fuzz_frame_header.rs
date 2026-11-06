#![no_main]

use anvil_types::FrameHeader;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|input: &[u8]| {
    let mut cursor = input;
    let _ = FrameHeader::decode(&mut cursor);
});
