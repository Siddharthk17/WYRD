#![no_main]

use anvil_types::Entry;
use bytes::Bytes;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|input: &[u8]| {
    let mut cursor = input;
    let _ = Entry::decode_from_slice(&mut cursor);

    let mut bytes_buf = Bytes::copy_from_slice(input);
    let _ = Entry::decode_from_bytes(&mut bytes_buf);
});
