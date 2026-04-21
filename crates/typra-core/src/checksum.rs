pub const CHECKSUM_KIND_CRC32C: u8 = 0;

pub fn crc32c(bytes: &[u8]) -> u32 {
    crc32c::crc32c(bytes)
}

pub fn crc32c_append(crc: u32, bytes: &[u8]) -> u32 {
    crc32c::crc32c_append(crc, bytes)
}
