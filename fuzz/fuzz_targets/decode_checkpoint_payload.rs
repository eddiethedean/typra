#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = modelvault_core::checkpoint::decode_checkpoint_payload(data);
});
