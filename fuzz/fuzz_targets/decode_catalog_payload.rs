#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = typra_core::catalog::decode_catalog_payload(data);
});
