//! Record payloads (v1/v2) and row/scalar codecs.

mod payload_v1;
mod payload_v2;
mod payload_v3;
mod row_value;
mod scalar;

pub use payload_v1::{
    decode_record_payload_v1, encode_record_payload_v1, DecodedRecord, OP_DELETE, OP_INSERT,
    OP_REPLACE,
};
pub use payload_v2::{
    decode_record_payload, encode_record_payload_v2, encode_record_payload_v2_op,
    RECORD_PAYLOAD_VERSION_V2,
};
pub use payload_v3::{
    decode_record_payload_any, encode_record_payload_v3, encode_record_payload_v3_op,
    RECORD_PAYLOAD_VERSION_V3,
};
pub use row_value::{encode_row_value, non_pk_defs_in_order, RowValue};
pub use scalar::{decode_tagged_scalar, encode_tagged_scalar, Cursor, ScalarValue};
