#![no_main]

use libfuzzer_sys::fuzz_target;

use modelvault_core::schema::{FieldDef, FieldPath, Type};

fuzz_target!(|data: &[u8]| {
    let pk_name = "id";
    let pk_ty = Type::Int64;
    let fields = [FieldDef {
        path: FieldPath(vec![std::borrow::Cow::Borrowed("id")]),
        ty: Type::Int64,
        constraints: vec![],
    }];
    let _ = modelvault_core::record::decode_record_payload(data, pk_name, &pk_ty, &fields);
});
