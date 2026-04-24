#![no_main]

use libfuzzer_sys::fuzz_target;

use typra_core::schema::{FieldDef, FieldPath, Type};

fuzz_target!(|data: &[u8]| {
    // Decode with a minimal schema context; errors are fine.
    let pk_name = "id";
    let pk_ty = Type::Int64;
    let fields = [FieldDef {
        path: FieldPath(vec![std::borrow::Cow::Borrowed("id")]),
        ty: Type::Int64,
        constraints: vec![],
    }];
    let _ = typra_core::record::decode_record_payload(data, pk_name, &pk_ty, &fields);
});
