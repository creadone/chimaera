//! Bounded single-frame decoding shared by ordinary readers and full verify.

use super::{MAX_BLOCK_BYTES, MAX_STORED_OVERHEAD};
use crate::error::{Result, corrupt};

pub(super) fn decode(stored: &[u8], expected_size: usize) -> Result<Vec<u8>> {
    if expected_size == 0
        || expected_size > MAX_BLOCK_BYTES
        || stored.len() > MAX_BLOCK_BYTES + MAX_STORED_OVERHEAD
    {
        return Err(corrupt("Chimaera v1 zstd block exceeds size limit"));
    }
    // Validate the frame and its declared output before allocating. Reject
    // unknown content size, skippable frames, concatenation and trailing bytes.
    let content_size = zstd::zstd_safe::get_frame_content_size(stored)
        .map_err(|_| corrupt("invalid Chimaera v1 zstd frame header"))?;
    if content_size != Some(expected_size as u64) {
        return Err(corrupt("Chimaera v1 zstd content size mismatch"));
    }
    let frame_size = zstd::zstd_safe::find_frame_compressed_size(stored)
        .map_err(|_| corrupt("invalid Chimaera v1 zstd frame bounds"))?;
    if frame_size != stored.len() {
        return Err(corrupt("Chimaera v1 zstd frame count or length mismatch"));
    }
    // The single-pass decoder uses this bounded output buffer as history. It
    // does not allocate a streaming window from untrusted frame metadata.
    let decoded = zstd::bulk::decompress(stored, expected_size)
        .map_err(|error| corrupt(format!("invalid Chimaera v1 zstd payload: {error}")))?;
    if decoded.len() != expected_size {
        return Err(corrupt("Chimaera v1 zstd decoded size mismatch"));
    }
    Ok(decoded)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Error;

    #[test]
    fn zstd_decode_rejects_unbounded_ambiguous_and_truncated_frames() -> Result<()> {
        let raw = vec![42; 16 * 1024];
        let stored = zstd::bulk::compress(&raw, 3)?;
        assert_eq!(decode(&stored, raw.len())?, raw);
        for expected in [0, raw.len() - 1, raw.len() + 1, MAX_BLOCK_BYTES + 1] {
            assert!(matches!(
                decode(&stored, expected),
                Err(Error::Corruption(_))
            ));
        }
        for end in 0..stored.len() {
            assert!(matches!(
                decode(&stored[..end], raw.len()),
                Err(Error::Corruption(_))
            ));
        }
        let mut appended = stored.clone();
        appended.push(0);
        assert!(matches!(
            decode(&appended, raw.len()),
            Err(Error::Corruption(_))
        ));
        appended.pop();
        appended.extend_from_slice(&stored);
        assert!(matches!(
            decode(&appended, raw.len()),
            Err(Error::Corruption(_))
        ));

        // An advertised huge output is rejected before a decoder/output buffer.
        // Zstd single-segment frame with an eight-byte content size and no blocks.
        let mut oversized = vec![0x28, 0xb5, 0x2f, 0xfd, 0xe0];
        oversized.extend_from_slice(&u64::MAX.to_le_bytes());
        assert!(matches!(decode(&oversized, 16), Err(Error::Corruption(_))));

        let mut encoder = zstd::bulk::Compressor::new(3)?;
        encoder.include_contentsize(false)?;
        let unknown_size = encoder.compress(&raw)?;
        assert!(matches!(
            decode(&unknown_size, raw.len()),
            Err(Error::Corruption(_))
        ));
        let skippable = [0x50, 0x2a, 0x4d, 0x18, 0, 0, 0, 0];
        assert!(matches!(decode(&skippable, 16), Err(Error::Corruption(_))));
        Ok(())
    }

    #[test]
    fn zstd_maximum_block_roundtrips_in_bounded_buffer() -> Result<()> {
        let mut state = 517457_u64;
        let raw = (0..MAX_BLOCK_BYTES)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                state as u8
            })
            .collect::<Vec<_>>();
        let stored = zstd::bulk::compress(&raw, 3)?;
        assert!(stored.len() <= MAX_BLOCK_BYTES + MAX_STORED_OVERHEAD);
        assert_eq!(decode(&stored, raw.len())?, raw);
        Ok(())
    }
}
