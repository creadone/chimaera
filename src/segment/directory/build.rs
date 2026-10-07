//! Build and durably install an immutable pack directory.

use super::*;
use crate::segment::SegmentDescriptor;

/// Borrowed for one publication; all scratch is charged to its work budget.
pub(crate) struct DirectoryUpdate<'a> {
    pub previous: &'a PublishedReadDirectoryDescriptor,
    pub previous_segments: &'a [SegmentDescriptor],
    pub current_segments: &'a [SegmentDescriptor],
}

impl OfflinePackDirectory {
    pub(crate) fn build(
        output: &Path,
        manifest: &Manifest,
        segments: &[Arc<Segment>],
    ) -> Result<OfflinePackDirectoryInfo> {
        Self::build_internal(
            output,
            manifest.storage_uuid,
            manifest.checkpoint_commit,
            manifest_crc32c(manifest)?,
            segments,
            MAX_ROOT_BYTES,
            None,
            false,
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn build_internal(
        output: &Path,
        storage_uuid: [u8; 16],
        identity_commit: u64,
        segments_crc32c: u32,
        segments: &[Arc<Segment>],
        maximum_root_bytes: usize,
        work_memory_governor: Option<&Arc<WorkMemoryGovernor>>,
        embed_descriptors: bool,
        update: Option<(&Path, &DirectoryUpdate<'_>)>,
    ) -> Result<OfflinePackDirectoryInfo> {
        if output.exists() {
            return Err(invalid("offline pack directory output already exists"));
        }
        let parent = output
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        if !parent.is_dir() {
            return Err(invalid("offline pack directory parent does not exist"));
        }

        let descriptors = update.map_or_else(
            || segments.iter().map(|s| s.descriptor()).collect::<Vec<_>>(),
            |(_, update)| update.current_segments.iter().collect(),
        );
        // The file length bounds the old locator count before allocating or
        // decoding its contents. Deleted locators make this conservative.
        let previous_bytes = update.map_or(0, |(_, u)| u.previous.file_size);
        let previous_capacity = usize::try_from(previous_bytes / LOCATOR_SIZE as u64)
            .map_err(|_| invalid("directory update size overflows"))?;
        let metadata_timer = cost::stage(CostStage::BuilderMetadata);
        let locator_count = segments
            .iter()
            .try_fold(previous_capacity, |count, segment| {
                count
                    .checked_add(segment.metadata()?.index.len())
                    .ok_or_else(|| invalid("offline pack directory locator count overflows"))
            })?;
        drop(metadata_timer);
        let descriptor_estimate = if embed_descriptors {
            descriptors.iter().try_fold(0_usize, |bytes, descriptor| {
                bytes
                    .checked_add(descriptor.relative_path.len().saturating_add(512))
                    .ok_or_else(|| invalid("published descriptor memory overflows"))
            })?
        } else {
            0
        };
        let build_bytes = locator_count
            .checked_mul(
                std::mem::size_of::<Locator>()
                    .saturating_add(std::mem::size_of::<Range<usize>>())
                    .saturating_add(std::mem::size_of::<PageDescriptor>())
                    .saturating_add(index::ENTRY_SIZE),
            )
            .and_then(|bytes| bytes.checked_mul(if embed_descriptors { 2 } else { 1 }))
            .and_then(|bytes| bytes.checked_add(MAX_PAGE_BYTES))
            .and_then(|bytes| bytes.checked_add(descriptor_estimate))
            // Old root/descriptor decode, verification maps and JSON scratch.
            .and_then(|bytes| {
                bytes.checked_add(usize::try_from(previous_bytes).ok()?.checked_mul(3)?)
            })
            .ok_or_else(|| invalid("offline pack directory build memory overflows"))?;
        if build_bytes > MAX_BUILD_BYTES {
            return Err(crate::error::Error::ResourceLimit(
                "offline pack directory build exceeds 512 MiB".into(),
            ));
        }
        let _work_memory = work_memory_governor
            .map(|governor| {
                // This optional index is built by the sole publisher. Waiting
                // for another job's scratch could prevent that job's output
                // from being published. Let the caller choose descriptor
                // storage when busy and retry the index on a later publication.
                governor
                    .try_reserve_background_with_headroom(
                        WorkMemoryClass::Compaction,
                        u64::try_from(build_bytes).unwrap_or(u64::MAX),
                        0,
                    )?
                    .ok_or_else(|| {
                        crate::error::Error::ResourceLimit(
                            "published read directory work-memory budget is busy or too small"
                                .into(),
                        )
                    })
            })
            .transpose()?;

        cost::add(CostCounter::BuildWorkBytesPeak, build_bytes);
        let descriptor_timer = cost::stage(CostStage::BuilderEncode);
        let descriptor_bytes = if embed_descriptors {
            let mut descriptors = descriptors.clone();
            descriptors.sort_unstable_by_key(|descriptor| descriptor.id);
            let bytes = serde_json::to_vec(&descriptors)?;
            if bytes.len() > descriptor_estimate || crc32c::crc32c(&bytes) != segments_crc32c {
                return Err(corrupt(
                    "published read directory descriptor section is invalid",
                ));
            }
            bytes
        } else {
            Vec::new()
        };

        drop(descriptor_timer);
        let metadata_timer = cost::stage(CostStage::BuilderMetadata);
        let mut locators = Vec::with_capacity(locator_count);
        if let Some((root, update)) = update {
            Self::visit_published(
                root,
                update.previous,
                storage_uuid,
                update.previous_segments,
                |locator| {
                    if update
                        .current_segments
                        .binary_search_by_key(&locator.segment_id, |d| d.id)
                        .is_ok()
                    {
                        if locators.len() == locator_count {
                            return Err(corrupt("directory update exceeds locator bound"));
                        }
                        locators.push(locator);
                    }
                    Ok(())
                },
            )?;
        }
        for segment in segments {
            let metadata = segment.metadata()?;
            locators.extend(metadata.index.iter().copied().map(|block| Locator {
                series: block.series,
                segment_id: segment.descriptor.id,
                block,
            }));
        }
        drop(metadata_timer);
        cost::add(CostCounter::Locators, locators.len());
        let encode_timer = cost::stage(CostStage::BuilderEncode);
        locators.sort_unstable_by_key(|locator| {
            (
                locator.series,
                locator.block.min_timestamp_us,
                locator.block.max_timestamp_us,
                locator.segment_id,
                locator.block.offset,
            )
        });

        let page_ranges = directory_page_ranges(&locators)?;
        cost::add(CostCounter::Pages, page_ranges.len());
        let root_bytes = index::root_entries(page_ranges.len())
            .checked_mul(index::ENTRY_SIZE)
            .ok_or_else(|| invalid("offline pack directory root length overflows"))?;
        if root_bytes > maximum_root_bytes {
            return Err(crate::error::Error::ResourceLimit(
                "offline pack directory root exceeds 1 MiB".into(),
            ));
        }
        let mut next_offset = u64::try_from(HEADER_SIZE.saturating_add(root_bytes))
            .map_err(|_| invalid("offline pack directory offset overflows"))?;
        let mut pages = Vec::with_capacity(page_ranges.len());
        let mut encoded_page = Vec::with_capacity(MAX_PAGE_BYTES);
        for range in &page_ranges {
            let chunk = &locators[range.clone()];
            encode_page(chunk, &mut encoded_page);
            let length = u32::try_from(encoded_page.len())
                .map_err(|_| invalid("offline pack directory page is too large"))?;
            pages.push(PageDescriptor {
                first_series: chunk
                    .first()
                    .ok_or_else(|| corrupt("offline pack directory page is empty"))?
                    .series,
                last_series: chunk
                    .last()
                    .ok_or_else(|| corrupt("offline pack directory page is empty"))?
                    .series,
                offset: next_offset,
                length,
                entries: u32::try_from(chunk.len())
                    .map_err(|_| invalid("offline pack directory entry count overflows"))?,
                crc32c: crc32c::crc32c(&encoded_page),
                level: 0,
                min_timestamp_us: chunk
                    .iter()
                    .map(|v| v.block.min_timestamp_us)
                    .min()
                    .unwrap_or(0),
                max_timestamp_us: chunk
                    .iter()
                    .map(|v| v.block.max_timestamp_us)
                    .max()
                    .unwrap_or(0),
            });
            next_offset = next_offset
                .checked_add(u64::from(length))
                .ok_or_else(|| invalid("offline pack directory length overflows"))?;
        }
        let leaf_page_count = pages.len();
        let built = index::build(pages, next_offset)?;
        let root = built.root;
        let total_length = built
            .end
            .checked_add(u64::try_from(descriptor_bytes.len()).unwrap_or(u64::MAX))
            .ok_or_else(|| invalid("offline pack directory length overflows"))?;
        let header = encode_header(
            storage_uuid,
            identity_commit,
            descriptors.len(),
            locators.len(),
            leaf_page_count,
            root.len(),
            crc32c::crc32c(&root),
            segments_crc32c,
            total_length,
            descriptor_bytes.len(),
        )?;

        let temporary = parent.join(format!(
            ".{}.{}.tmp",
            output
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("pack-directory"),
            Uuid::new_v4()
        ));
        let mut unpublished = UnpublishedFileGuard::new(output.to_path_buf());
        let mut unpublished_temporary = UnpublishedFileGuard::new(temporary.clone());
        drop(encode_timer);
        let write_result = (|| -> Result<()> {
            let write_timer = cost::stage(CostStage::BuilderWrite);
            let mut file = OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&temporary)?;
            unpublished_temporary.mark_present();
            file.write_all(&header)?;

            file.write_all(&root)?;
            for range in &page_ranges {
                let chunk = &locators[range.clone()];
                encode_page(chunk, &mut encoded_page);
                file.write_all(&encoded_page)?;
            }
            for branch in &built.branches {
                file.write_all(branch)?;
            }
            file.write_all(&descriptor_bytes)?;
            drop(write_timer);
            let _sync_timer = cost::stage(CostStage::BuilderSync);
            file.sync_all()?;

            fs::rename(&temporary, output)?;
            unpublished_temporary.commit();
            unpublished.mark_present();

            sync_directory(parent)?;
            Ok(())
        })();
        write_result?;
        unpublished.commit();

        let physical_files = descriptors
            .iter()
            .map(|descriptor| descriptor.relative_path.as_str())
            .collect::<HashSet<_>>()
            .len();
        Ok(OfflinePackDirectoryInfo {
            format: "TSPDR001",
            checkpoint_commit_id: identity_commit,
            logical_segments: descriptors.len(),
            physical_files,
            block_locators: u64::try_from(locators.len()).unwrap_or(u64::MAX),
            pages: leaf_page_count,
            root_bytes: u64::try_from(root.len()).unwrap_or(u64::MAX),
            artifact_bytes: total_length,
        })
    }
}
