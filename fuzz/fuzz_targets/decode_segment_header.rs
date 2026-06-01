#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = modelvault_core::segments::header::decode_segment_header(data);
});
