//! Bounded streaming copy of immutable regions; only ID/header CRC changes.
use super::{HEADER_SIZE, SegmentDescriptor, index, put_u32, put_u64, sync_segment_pack_group};
use crate::error::{Result, corrupt, invalid};
use crate::persistence::files::FilePermitPool;
use crate::persistence::wal::UnpublishedFileGuard;
use std::fs::{self, File};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

pub(crate) const COPY_BUFFER_BYTES: usize = 64 * 1024;

/// Owned by one coordinator. No file descriptor remains open between steps.
/// The caller reserves the buffer/descriptors and holds the input snapshot.
pub(crate) struct Relocation {
    uuid: [u8; 16],
    inputs: Vec<SegmentDescriptor>,
    outputs: Vec<SegmentDescriptor>,
    temporary_path: PathBuf,
    final_path: PathBuf,
    temporary: UnpublishedFileGuard,
    unpublished: UnpublishedFileGuard,
    buffer: Vec<u8>,
    input: usize,
    offset: u64,
    copied: u64,
    total: u64,
}

impl Relocation {
    pub(crate) fn new(
        root: &Path,
        uuid: [u8; 16],
        inputs: Vec<SegmentDescriptor>,
        first_id: u64,
    ) -> Result<Self> {
        let first = inputs.first().ok_or_else(|| invalid("empty relocation"))?;
        if first_id == 0
            || inputs
                .iter()
                .any(|d| d.shard_id != first.shard_id || d.file_size < HEADER_SIZE as u64)
        {
            return Err(invalid("invalid relocation inputs"));
        }
        let relative = format!(
            "segments/packs/PACK-S{:04}-{first_id:020}.tsp",
            first.shard_id
        );
        let temporary_path = root.join(format!("tmp/PACK-{first_id:020}.tmp"));
        let final_path = root.join(&relative);
        let mut total = 0_u64;
        let outputs = inputs
            .iter()
            .enumerate()
            .map(|(i, d)| {
                let mut output = d.clone();
                output.id = first_id
                    .checked_add(i as u64)
                    .ok_or_else(|| corrupt("relocation id overflow"))?;
                output.relative_path.clone_from(&relative);
                output.pack_id = Some(first_id);
                output.file_offset = total;
                total = total
                    .checked_add(d.file_size)
                    .ok_or_else(|| corrupt("relocation size overflow"))?;
                Ok(output)
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            uuid,
            inputs,
            outputs,
            temporary: UnpublishedFileGuard::new(temporary_path.clone()),
            unpublished: UnpublishedFileGuard::new(final_path.clone()),
            temporary_path,
            final_path,
            buffer: vec![0; COPY_BUFFER_BYTES],
            input: 0,
            offset: 0,
            copied: 0,
            total,
        })
    }

    pub(crate) fn remaining(&self) -> u64 {
        self.total - self.copied
    }

    pub(crate) fn step(&mut self, root: &Path, pool: &FilePermitPool, limit: u64) -> Result<u64> {
        if limit < HEADER_SIZE as u64 {
            return Err(invalid("relocation step too small"));
        }
        let _output_permit = pool.acquire()?;
        let mut output = File::options()
            .write(true)
            .create_new(self.copied == 0)
            .open(&self.temporary_path)?;
        self.temporary.mark_present();
        output.seek(SeekFrom::Start(self.copied))?;
        let before = self.copied;
        while self.input < self.inputs.len() && self.copied - before < limit {
            if self.offset == 0 && limit - (self.copied - before) < HEADER_SIZE as u64 {
                break;
            }
            let descriptor = &self.inputs[self.input];
            let path = root.join(&descriptor.relative_path);
            let _input_permit = pool.acquire()?;
            let mut source = File::open(&path)?;
            source.seek(SeekFrom::Start(
                descriptor
                    .file_offset
                    .checked_add(self.offset)
                    .ok_or_else(|| corrupt("relocation offset overflow"))?,
            ))?;
            if self.offset == 0 {
                let mut header = [0; HEADER_SIZE];
                source.read_exact(&mut header)?;
                index::validate_header(&header, self.uuid, descriptor, &path)?;
                put_u64(&mut header, 28, self.outputs[self.input].id);
                let checksum = crc32c::crc32c(&header[..108]);
                put_u32(&mut header, 108, checksum);
                output.write_all(&header)?;
                self.offset += HEADER_SIZE as u64;
                self.copied += HEADER_SIZE as u64;
            }
            while self.offset < descriptor.file_size && self.copied - before < limit {
                let bytes = (descriptor.file_size - self.offset)
                    .min(limit - (self.copied - before))
                    .min(self.buffer.len() as u64) as usize;
                source.read_exact(&mut self.buffer[..bytes])?;
                output.write_all(&self.buffer[..bytes])?;
                self.offset += bytes as u64;
                self.copied += bytes as u64;
            }
            if self.offset == descriptor.file_size {
                self.input += 1;
                self.offset = 0;
            }
        }

        Ok(self.copied - before)
    }

    pub(crate) fn finish(
        mut self,
        root: &Path,
        pool: &FilePermitPool,
    ) -> Result<(Vec<SegmentDescriptor>, UnpublishedFileGuard)> {
        if self.remaining() != 0 {
            return Err(invalid("unfinished relocation"));
        }
        {
            let _permit = pool.acquire()?;
            let file = File::options().write(true).open(&self.temporary_path)?;
            if file.metadata()?.len() != self.total {
                return Err(corrupt("relocation output size"));
            }

            file.sync_all()?;
        }

        fs::rename(&self.temporary_path, &self.final_path)?;
        self.temporary.commit();
        self.unpublished.mark_present();

        sync_segment_pack_group(root)?;
        Ok((self.outputs, self.unpublished))
    }
}
