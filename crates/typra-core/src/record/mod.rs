//! Record payloads and scalar values (v1).

mod payload_v1;
mod scalar;

pub use payload_v1::{
    decode_record_payload_v1, encode_record_payload_v1, DecodedRecord, OP_INSERT,
};
pub use scalar::{decode_tagged_scalar, encode_tagged_scalar, Cursor, ScalarValue};
