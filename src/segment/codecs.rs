//! Block timestamp and value encoding.

use crate::catalog::TimestampCodec;
use crate::error::{Result, invalid};
use crate::persistence::wal::{put_i64, put_u64};
use crate::types::VersionedPoint;

pub(super) fn encode_timestamps(
    operations: &[VersionedPoint],
    codec: TimestampCodec,
) -> Result<Vec<u8>> {
    let Some(first) = operations.first() else {
        return Ok(Vec::new());
    };
    if codec == TimestampCodec::RawInt64 {
        let mut result = vec![0_u8; operations.len() * 8];
        for (position, point) in operations.iter().enumerate() {
            if position > 0 && point.key.timestamp_us < operations[position - 1].key.timestamp_us {
                return Err(invalid("block timestamps are not monotonic"));
            }
            put_i64(&mut result, position * 8, point.key.timestamp_us);
        }
        return Ok(result);
    }
    let mut result = vec![0_u8; 8];
    put_i64(&mut result, 0, first.key.timestamp_us);
    let mut previous = first.key.timestamp_us;
    let mut previous_delta = 0_i64;
    for (position, point) in operations.iter().enumerate().skip(1) {
        let delta = point
            .key
            .timestamp_us
            .checked_sub(previous)
            .ok_or_else(|| invalid("timestamp delta overflow"))?;
        if delta < 0 {
            return Err(invalid("block timestamps are not monotonic"));
        }
        let component = match codec {
            TimestampCodec::RawInt64 => unreachable!("raw timestamps returned above"),
            TimestampCodec::DeltaVarint => delta,
            TimestampCodec::DeltaOfDelta if position == 1 => delta,
            TimestampCodec::DeltaOfDelta => delta
                .checked_sub(previous_delta)
                .ok_or_else(|| invalid("timestamp delta-of-delta overflow"))?,
        };
        encode_varint(zigzag_encode(component), &mut result);
        previous = point.key.timestamp_us;
        previous_delta = delta;
    }
    Ok(result)
}

pub(super) fn encode_raw_values(operations: &[VersionedPoint]) -> Vec<u8> {
    let put_count = operations.len();
    let mut result = vec![0_u8; put_count * 8];
    for (position, point) in operations.iter().enumerate() {
        put_u64(&mut result, position * 8, point.value_bits);
    }
    result
}

pub(super) fn encode_gorilla_values(operations: &[VersionedPoint]) -> Vec<u8> {
    let mut puts = operations.iter();
    let Some(first) = puts.next() else {
        return Vec::new();
    };
    let mut result = vec![0_u8; 8];
    put_u64(&mut result, 0, first.value_bits);
    let mut bits = BitWriter::default();
    let mut previous = first.value_bits;
    let mut window: Option<(u32, u32)> = None;
    for point in puts {
        let xor = previous ^ point.value_bits;
        if xor == 0 {
            bits.write_bit(false);
        } else {
            bits.write_bit(true);
            let leading = xor.leading_zeros();
            let trailing = xor.trailing_zeros();
            if let Some((old_leading, old_trailing)) = window
                && leading >= old_leading
                && trailing >= old_trailing
            {
                bits.write_bit(false);
                bits.write_bits(xor >> old_trailing, 64 - old_leading - old_trailing);
            } else {
                bits.write_bit(true);
                let significant = 64 - leading - trailing;
                bits.write_bits(u64::from(leading), 6);
                bits.write_bits(u64::from(significant - 1), 6);
                bits.write_bits(xor >> trailing, significant);
                window = Some((leading, trailing));
            }
        }
        previous = point.value_bits;
    }
    result.extend_from_slice(&bits.finish());
    result
}

#[derive(Default)]
pub(super) struct BitWriter {
    pub(super) bytes: Vec<u8>,
    pub(super) current: u8,
    pub(super) used: u8,
}

impl BitWriter {
    pub(super) fn write_bit(&mut self, value: bool) {
        self.current |= u8::from(value) << (7 - self.used);
        self.used += 1;
        if self.used == 8 {
            self.bytes.push(self.current);
            self.current = 0;
            self.used = 0;
        }
    }

    pub(super) fn write_bits(&mut self, value: u64, width: u32) {
        for shift in (0..width).rev() {
            self.write_bit((value >> shift) & 1 == 1);
        }
    }

    pub(super) fn finish(mut self) -> Vec<u8> {
        if self.used > 0 {
            self.bytes.push(self.current);
        }
        self.bytes
    }
}

pub(super) fn encode_varint(mut value: u64, output: &mut Vec<u8>) {
    while value >= 0x80 {
        output.push((value as u8 & 0x7f) | 0x80);
        value >>= 7;
    }
    output.push(value as u8);
}

pub(super) fn zigzag_encode(value: i64) -> u64 {
    ((value as u64) << 1) ^ ((value >> 63) as u64)
}
