#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    // Scan a random byte image as if it were a file. Errors are fine; panics are not.
    let mut store = typra_core::storage::VecStore::from_vec(data.to_vec());
    let _ = typra_core::segments::reader::scan_segments(&mut store, 0);
});
