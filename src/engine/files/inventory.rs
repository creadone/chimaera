//! Streaming recovery checks and explicitly materialized administrative images.
use super::*;
use crate::aggregate::archive::AggregateArchiveDescriptor;
use crate::persistence::catalog::records::{DomainEntry, PackEntry};
use crate::persistence::manifest::{Manifest, ManifestArtifact, ManifestArtifactKind};

impl View {
    pub fn validate_inventory(&self, manifest: &Manifest) -> Result<()> {
        let semantic_catalog = manifest.catalog.snapshot();
        let mut counts = [0_u64; 3];
        let mut bytes = [0_u64; 3];
        self.all(|entry| {
            let d = &entry.descriptor;
            if d.id == 0
                || d.id >= manifest.next_segment_id
                || d.level > 2
                || d.window_start_us >= d.window_end_us
                || d.min_timestamp_us > d.max_timestamp_us
                || d.min_timestamp_us < d.window_start_us
                || d.max_timestamp_us >= d.window_end_us
                || d.shard_id >= manifest.virtual_shards
                || d.policy_id == 0
                || d.max_commit > manifest.materialized_commit()
                || (d.policy_id != 1 && semantic_catalog.policy_by_id(d.policy_id).is_none())
            {
                return Err(corrupt("invalid catalog segment identity or commit"));
            }
            let by_id = self
                .lookup(d.id)?
                .ok_or_else(|| corrupt("missing segment id index"))?;
            if by_id.descriptor != *d
                || by_id.directory_id != entry.directory_id
                || by_id.catalog_indexed != entry.catalog_indexed
            {
                return Err(corrupt("segment id index mismatch"));
            }
            if entry.catalog_indexed || entry.directory_id.is_some() {
                return Err(corrupt("obsolete read directory membership"));
            }
            let physical = self
                .snapshot
                .get(records::pack_key(d), None)?
                .ok_or_else(|| corrupt("missing physical pack"))?;
            let physical: PackEntry = records::decode(&physical)?;
            if physical.references == 0
                || physical.relative_path != d.relative_path
                || d.file_offset
                    .checked_add(d.file_size)
                    .is_none_or(|end| end > physical.file_size)
            {
                return Err(corrupt("segment pack bounds"));
            }
            // Validates physical path/header/footer without retaining a history-sized set of handles.
            self.resources.open(d.clone(), None)?;
            counts[d.level as usize] += 1;
            bytes[d.level as usize] += d.file_size;
            Ok(())
        })?;
        if counts != self.totals.segments || bytes != self.totals.payload_bytes {
            return Err(corrupt("catalog segment totals mismatch"));
        }
        for kind in [records::BLOCK, records::DIRECTORY] {
            let (lo, hi) = records::kind_scopes(kind);
            if self
                .snapshot
                .visit(lo, hi, i64::MIN, i64::MAX, None, |_| Ok(()))?
                != 0
            {
                return Err(corrupt("obsolete read directory records"));
            }
        }
        let mut physical_files = 0;
        let mut physical_bytes = 0;
        let mut references = 0_u64;
        let mut packed_members = 0_u64;
        let mut candidate_count = 0_u64;
        let mut candidate_live = 0_u64;
        let mut candidate_dead = 0_u64;
        for kind in [records::PACK, records::SINGLE] {
            self.visit_kind(kind, |key, entry: PackEntry| {
                if entry.references == 0 {
                    return Err(corrupt("live pack without references"));
                }
                if entry.live_bytes > entry.file_size {
                    return Err(corrupt("pack live byte bounds"));
                }
                if kind == records::PACK {
                    let mut after = 0;
                    let mut count = 0_u64;
                    let mut live = 0_u64;
                    loop {
                        let members = self.pack_members(key.id, after, 64)?;
                        if members.is_empty() {
                            break;
                        }
                        for member in members {
                            after = member.descriptor.id;
                            count += 1;
                            live = live
                                .checked_add(member.descriptor.file_size)
                                .ok_or_else(|| corrupt("pack live bytes overflow"))?;
                        }
                    }
                    if count != entry.references || live != entry.live_bytes {
                        return Err(corrupt("pack member totals mismatch"));
                    }
                    packed_members += count;
                } else if entry.references != 1 || entry.live_bytes != entry.file_size {
                    return Err(corrupt("single segment physical totals"));
                }
                let eligible = kind == records::PACK && entry.needs_repack();
                if eligible != entry.repack_since_ms.is_some() {
                    return Err(corrupt("pack repack eligibility mismatch"));
                }
                if let Some(candidate) = entry.candidate_key(key.id)? {
                    let row = self
                        .snapshot
                        .get(candidate, None)?
                        .ok_or_else(|| corrupt("missing repack candidate"))?;
                    if records::decode::<super::Key>(&row)? != key {
                        return Err(corrupt("repack candidate points to wrong pack"));
                    }
                    candidate_count += 1;
                    candidate_live += entry.live_bytes;
                    candidate_dead += entry.file_size - entry.live_bytes;
                }
                let artifact = ManifestArtifact {
                    relative_path: entry.relative_path.clone().into(),
                    kind: ManifestArtifactKind::Segment,
                    minimum_size: entry.file_size,
                    exact_size: Some(entry.file_size),
                    pack_id: (kind == records::PACK).then_some(key.id),
                };
                validate_path(&entry.relative_path, "segments")?;
                artifact.validate_at(&self.resources.root)?;
                references = references
                    .checked_add(entry.references)
                    .ok_or_else(|| corrupt("pack reference count overflow"))?;
                physical_files += 1;
                physical_bytes += entry.file_size;
                Ok(())
            })?;
        }
        if references != counts.iter().sum::<u64>()
            || physical_files != self.totals.physical_files
            || physical_bytes != self.totals.physical_bytes
        {
            return Err(corrupt("catalog physical totals mismatch"));
        }
        for (kind, expected) in [
            (records::PACK_MEMBER, packed_members),
            (records::REPACK, candidate_count),
        ] {
            let (lo, hi) = records::kind_scopes(kind);
            if self
                .snapshot
                .visit(lo, hi, i64::MIN, i64::MAX, None, |_| Ok(()))?
                != expected
            {
                return Err(corrupt("orphan repack metadata"));
            }
        }
        if (candidate_count, candidate_live, candidate_dead)
            != (
                self.totals.repack_files,
                self.totals.repack_live_bytes,
                self.totals.repack_dead_bytes,
            )
        {
            return Err(corrupt("repack totals mismatch"));
        }
        if self.totals.directories != 0
            || self.totals.directory_bytes != 0
            || self.totals.directory_segments != 0
        {
            return Err(corrupt("obsolete read directory totals"));
        }
        let mut archives = 0;
        let mut archive_bytes = 0;
        let mut previous_archive: Option<AggregateArchiveDescriptor> = None;
        self.visit_kind(records::ARCHIVE, |key, d: AggregateArchiveDescriptor| {
            d.validate(manifest.virtual_shards, manifest.next_segment_id)?;
            validate_path(&d.relative_path, "aggregates")?;
            if records::archive_key(&d) != key
                || self.object_key(records::ARCHIVE, d.id)? != Some(key)
            {
                return Err(corrupt("archive index"));
            }
            let policy = manifest.catalog.snapshot();
            let policy = policy
                .policy_by_id(d.policy_id)
                .ok_or_else(|| corrupt("archive policy"))?;
            if policy.aggregate_retention_us.is_none()
                || d.resolutions
                    .iter()
                    .any(|r| !policy.retained_aggregate_resolutions.contains(r))
            {
                return Err(corrupt("archive policy resolutions"));
            }
            match &previous_archive {
                Some(old)
                    if old.policy_id == d.policy_id
                        && old.shard_id == d.shard_id
                        && old.coverage_from_us == d.coverage_from_us =>
                {
                    if old.coverage_to_us != d.coverage_to_us
                        || old.chunk_count != d.chunk_count
                        || old.chunk_index + 1 != d.chunk_index
                    {
                        return Err(corrupt("archive chunk set disagrees"));
                    }
                }
                old => {
                    if old
                        .as_ref()
                        .is_some_and(|old| old.chunk_index + 1 != old.chunk_count)
                        || d.chunk_index != 0
                    {
                        return Err(corrupt("archive chunk set incomplete"));
                    }
                    if old.as_ref().is_some_and(|old| {
                        old.policy_id == d.policy_id
                            && old.shard_id == d.shard_id
                            && old.coverage_to_us > d.coverage_from_us
                    }) {
                        return Err(corrupt("archive coverage overlaps"));
                    }
                }
            }
            previous_archive = Some(d.clone());
            archives += 1;
            archive_bytes += d.file_size;
            self.resources.archive(d)?;
            Ok(())
        })?;
        if previous_archive.is_some_and(|old| old.chunk_index + 1 != old.chunk_count) {
            return Err(corrupt("archive final chunk missing"));
        }
        if archives != self.totals.archives || archive_bytes != self.totals.archive_bytes {
            return Err(corrupt("archive totals"));
        }
        let mut retired_files = 0;
        let mut retired_bytes = 0;
        let mut oldest = None;
        self.visit_kind(records::RETIRED, |key, entry: records::RetiredEntry| {
            let kind = ((key.scope >> 16) & u64::from(u32::MAX)) as u8;
            let prefix = match kind {
                records::ARCHIVE => "aggregates",
                records::DIRECTORY => "segments/directories",
                records::PACK | records::SINGLE => "segments",
                _ => return Err(corrupt("retired file kind")),
            };
            validate_path(&entry.relative_path, prefix)?;
            if entry.generation > self.root().generation || key.start != entry.generation as i64 {
                return Err(corrupt("retirement generation"));
            }
            retired_files += 1;
            retired_bytes += entry.file_size;
            oldest = Some(oldest.map_or(entry.retired_ms, |ms: u64| ms.min(entry.retired_ms)));
            Ok(())
        })?;
        if retired_files != self.totals.retired_files
            || retired_bytes != self.totals.retired_bytes
            || oldest != self.totals.oldest_retired_ms
        {
            return Err(corrupt("retirement totals"));
        }
        let mut domain_counts = [0_u64; 3];
        self.visit_kind(records::DOMAIN, |key, d: DomainEntry| {
            let domain = records::domain_from_key(key)?;
            let mut actual = DomainEntry {
                history_target_bytes: d.history_target_bytes,
                history_trigger: d.history_trigger,
                history_window_us: d.history_window_us,
                minimum_timestamp: i64::MAX,
                maximum_timestamp: i64::MIN,
                ..Default::default()
            };
            self.domain(domain, |entry| {
                super::publication::accumulate(&mut actual, &entry, d.history_target_bytes)
            })?;
            actual.history_merge = actual.counts[2] >= d.history_trigger.max(2) as u64
                && actual
                    .minimum_l2_bytes
                    .is_some_and(|size| size < d.history_target_bytes);
            if actual.history_merge && actual.counts[0] == 0 && actual.counts[1] == 0 {
                let mut cursor = super::Cursor::new(self, domain);
                actual.history_merge = !crate::engine::compaction::history::select(
                    || cursor.next().map(|row| row.map(|e| e.descriptor)),
                    i64::MAX,
                    d.history_target_bytes,
                    d.history_trigger,
                    d.history_window_us,
                    self.snapshot.work_governor(),
                )?
                .is_empty();
            }
            if actual.count() == 0 || actual != d {
                return Err(corrupt("domain summary mismatch"));
            }
            for (total, count) in domain_counts.iter_mut().zip(actual.counts) {
                *total += count;
            }
            let pending = self
                .snapshot
                .get(records::domain_key(records::PENDING, domain), None)?;
            let expected = d.pending();
            if pending.is_some() != expected
                || pending
                    .as_ref()
                    .map(records::decode::<DomainEntry>)
                    .transpose()?
                    .is_some_and(|value| value != d)
            {
                return Err(corrupt("pending domain summary mismatch"));
            }
            Ok(())
        })?;
        if domain_counts != counts {
            return Err(corrupt("missing domain summary"));
        }
        self.visit_kind(records::PENDING, |key, _: DomainEntry| {
            let domain = records::domain_from_key(key)?;
            if self.domain_metadata(domain)?.is_none() {
                return Err(corrupt("orphan pending domain"));
            }
            Ok(())
        })?;
        let mut indexes = 0_u64;
        self.visit_kind(records::ID, |key, target: Key| {
            let kind = match (key.scope >> 16) & 0xffffffff {
                0 => records::SEGMENT,
                value if value == u64::from(records::DIRECTORY) => records::DIRECTORY,
                value if value == u64::from(records::ARCHIVE) => records::ARCHIVE,
                _ => return Err(corrupt("invalid object index kind")),
            };
            if key.start != 0
                || key.scope & 0xffff != 0
                || target.scope >> 56 != u64::from(kind)
                || key.id != target.id
                || self.snapshot.get(target, None)?.is_none()
            {
                return Err(corrupt("orphan object index"));
            }
            indexes += 1;
            Ok(())
        })?;
        if indexes != counts.iter().sum::<u64>() + archives {
            return Err(corrupt("object index count"));
        }
        Ok(())
    }
    pub fn reconfigure(
        &mut self,
        manifest: &mut Manifest,
        catalog: &catalog::Catalog,
        work: &Arc<crate::memory::work::WorkMemoryGovernor>,
        options: &crate::OpenOptions,
    ) -> Result<()> {
        if manifest.catalog_history_target_bytes == options.history_segment_target_bytes
            && manifest.catalog_history_trigger == options.history_rewrite_trigger
            && manifest.catalog_history_window_us == options.history_window.as_micros() as i64
        {
            return Ok(());
        }
        let mut after = None;
        loop {
            let page = self.snapshot.page(records::DOMAIN, after, 128)?;
            if page.is_empty() {
                break;
            }
            after = page.last().map(|row| row.key);
            let mut builder = super::publication::Builder::new(self, work)?;
            for row in page.iter() {
                builder.refresh_domain(records::domain_from_key(row.key)?)?;
            }
            let proposal = builder.finish(
                options.l0_compaction_trigger,
                options.history_rewrite_trigger,
                options.history_segment_target_bytes,
                options.history_window.as_micros() as i64,
            )?;
            let next = proposal.prepare(catalog, &self.snapshot)?;
            manifest.file_catalog = next.root;
            manifest.file_totals = proposal.totals.clone();
            let mut committed = false;
            let result = manifest.persist_with_commit(&self.resources.root, || committed = true);
            if committed {
                catalog.committed(next.root, result.is_ok())?;
            }
            result?;
            self.snapshot = next;
            self.totals = proposal.totals;
            catalog.collect()?;
        }
        manifest.catalog_history_target_bytes = options.history_segment_target_bytes;
        manifest.catalog_history_trigger = options.history_rewrite_trigger;
        manifest.catalog_history_window_us = options.history_window.as_micros() as i64;
        manifest.persist(&self.resources.root)?;
        Ok(())
    }
    fn object_key(&self, kind: u8, id: u64) -> Result<Option<Key>> {
        self.snapshot
            .get(records::object_id_key(kind, id), None)?
            .as_ref()
            .map(records::decode)
            .transpose()
    }
    pub fn references_path(&self, path: &std::path::Path) -> Result<bool> {
        let relative = path
            .strip_prefix(&self.resources.root)
            .map_err(|_| corrupt("artifact outside storage"))?;
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            return Ok(false);
        };
        let object = if let Some(id) = name
            .strip_prefix("READ-DIRECTORY-")
            .and_then(|s| s.strip_suffix(".tspd"))
        {
            let Ok(id) = id.parse() else { return Ok(false) };
            self.object_key(records::DIRECTORY, id)?
        } else if let Some(id) = name
            .strip_prefix("AGG-")
            .and_then(|s| s.strip_suffix(".tsa"))
        {
            let Ok(id) = id.parse() else { return Ok(false) };
            self.object_key(records::ARCHIVE, id)?
        } else if let Some(tail) = name
            .strip_prefix("PACK-S")
            .and_then(|s| s.strip_suffix(".tsp"))
        {
            let Some((_, id)) = tail.split_once('-') else {
                return Ok(false);
            };
            let Ok(id) = id.parse() else { return Ok(false) };
            Some(Key {
                series: Default::default(),
                scope: records::scope(records::PACK, 0, 0),
                start: 0,
                id,
            })
        } else if let Some(tail) = name
            .strip_prefix("SEG-")
            .and_then(|s| s.strip_suffix(".tss"))
        {
            let Some((id, _)) = tail.split_once("-L") else {
                return Ok(false);
            };
            let Ok(id) = id.parse() else { return Ok(false) };
            Some(Key {
                series: Default::default(),
                scope: records::scope(records::SINGLE, 0, 0),
                start: 0,
                id,
            })
        } else {
            return Ok(false);
        };
        let Some(key) = object else { return Ok(false) };
        let Some(row) = self.snapshot.get(key, None)? else {
            return Ok(false);
        };
        let value: serde_json::Value = records::decode(&row)?;
        Ok(value
            .get("relative_path")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|stored| std::path::Path::new(stored) == relative))
    }
    #[allow(clippy::type_complexity)]
    pub fn materialize(
        &self,
        manifest: &mut Manifest,
        commit: u64,
    ) -> Result<(
        Vec<Arc<Segment>>,
        Vec<Arc<AggregateArchive>>,
        MemoryReservation,
    )> {
        let memory = manifest.materialize_catalog(
            &self.resources.root,
            &self.snapshot,
            &self.resources.governor,
        )?;
        let mut segments = Vec::new();
        for d in &manifest.segments {
            if d.max_commit > commit {
                return Err(corrupt("administrative snapshot exceeds checkpoint"));
            }
            segments.push(self.resources.open(d.clone(), None)?);
        }
        let archives = manifest
            .aggregate_archives
            .iter()
            .map(|d| self.resources.archive(d.clone()))
            .collect::<Result<_>>()?;
        Ok((segments, archives, memory))
    }
}
fn validate_path(value: &str, prefix: &str) -> Result<()> {
    let path = std::path::Path::new(value);
    if !path.starts_with(prefix)
        || path
            .components()
            .any(|c| !matches!(c, std::path::Component::Normal(_)))
    {
        return Err(corrupt("catalog artifact path"));
    }
    Ok(())
}
