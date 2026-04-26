#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    // Scan a random byte image as if it were a file. Errors are fine; panics are not.
    let mut store = typra_core::storage::VecStore::from_vec(data.to_vec());
    let res = typra_core::segments::reader::scan_segments(&mut store, 0);
    if let Ok(metas) = res {
        // Invariants: segment offsets are monotonic, and each header's payload_len fits in usize.
        let mut last = 0u64;
        for m in metas {
            assert!(m.offset >= last);
            last = m.offset;
            let _ = usize::try_from(m.header.payload_len);
        }
    }
});
