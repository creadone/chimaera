//! Bounded single-frame decoding shared by ordinary readers and full verify.

use super::{MAX_BLOCK_BYTES, MAX_STORED_OVERHEAD};
use crate::error::{Result, corrupt};

pub(super) fn decode(stored: &[u8], expected_size: usize) -> Result<Vec<u8>> {
    if expected_size == 0
        || expected_size > MAX_BLOCK_BYTES
        || stored.len() > MAX_BLOCK_BYTES + MAX_STORED_OVERHEAD
    {
        return Err(corrupt("Chimaera zstd block exceeds size limit"));
    }
    // Validate the frame and its declared output before allocating. Reject
    // unknown content size, skippable frames, concatenation and trailing bytes.
    let content_size = zstd::zstd_safe::get_frame_content_size(stored)
        .map_err(|_| corrupt("invalid Chimaera zstd frame header"))?;
    if content_size != Some(expected_size as u64) {
        return Err(corrupt("Chimaera zstd content size mismatch"));
    }
    let frame_size = zstd::zstd_safe::find_frame_compressed_size(stored)
        .map_err(|_| corrupt("invalid Chimaera zstd frame bounds"))?;
    if frame_size != stored.len() {
        return Err(corrupt("Chimaera zstd frame count or length mismatch"));
    }
    // The single-pass decoder uses this bounded output buffer as history. It
    // does not allocate a streaming window from untrusted frame metadata.
    let decoded = zstd::bulk::decompress(stored, expected_size)
        .map_err(|error| corrupt(format!("invalid Chimaera zstd payload: {error}")))?;
    if decoded.len() != expected_size {
        return Err(corrupt("Chimaera zstd decoded size mismatch"));
    }
    Ok(decoded)
}
