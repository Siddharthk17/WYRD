#![no_main]

use anvil_types::{decode_varint_i64, decode_varint_u64};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let mut cursor = data;
    let _ = decode_varint_u64(&mut cursor);

    let mut cursor_signed = data;
    let _ = decode_varint_i64(&mut cursor_signed);
});
