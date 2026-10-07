//! Catalog, time policies, retention, and write-order validation.

use super::{Inner, PublishCommand, State, Storage, WriteCommand};
use crate::catalog::{
    BucketDefinition, CatalogSnapshot, CompressionProfile, PolicySelector, RetentionInfo,
    SeriesRef, SeriesStorageInfo, SpaceDefinition, StoragePolicy, StoragePolicyOptions,
    validate_name,
};
use crate::engine::lifecycle::admission::OverloadReason;
use crate::error::{Error, Result, invalid};
use crate::persistence::layout::Domain;
use crate::types::{DAY_US, Point, SeriesKey};
use crossbeam_channel::bounded;
use std::collections::HashMap;

impl Storage {
    /// Return an immutable snapshot of the durable catalog.
    ///
    /// # Errors
    ///
    /// Returns an error if storage is closed or an internal lock is poisoned.
    pub fn catalog(&self) -> Result<CatalogSnapshot> {
        self.ensure_available()?;
        Ok(self.inner.state.read()?.catalog.clone())
    }

    /// Return effective raw/aggregate retention cutoffs and the observed event end.
    /// These are not complete write-admission bounds: timestamp order, maximum
    /// lateness, and future-skew checks apply separately.
    ///
    /// # Errors
    ///
    /// Returns an error for an unknown policy, unavailable storage, or a poisoned
    /// internal lock.
    pub fn retention_info(&self, policy: impl Into<PolicySelector>) -> Result<RetentionInfo> {
        self.ensure_available()?;
        let selector = policy.into();
        let now_us = self.inner.options.clock.now_us();
        let mut state = self.inner.state.write()?;
        state.maintenance_now_us = state.maintenance_now_us.max(now_us);
        let policy = match &selector {
            PolicySelector::Id(id) => state.catalog.policy_by_id(*id),
            PolicySelector::Name(name) => state.catalog.policy_by_name(name),
        }
        .cloned()
        .ok_or_else(|| Error::NotFound("storage policy".into()))?;
        let maintenance_now_us = state.maintenance_now_us;
        let raw_available_from_us =
            advance_retention_cutoff(&mut state, &policy, maintenance_now_us);
        let aggregate_available_from_us =
            advance_aggregate_retention_cutoff(&mut state, &policy, maintenance_now_us);
        let available_to_us = state
            .event_watermarks
            .get(&policy.id)
            .copied()
            .and_then(|timestamp| timestamp.checked_add(1));
        Ok(RetentionInfo {
            policy_id: policy.id,
            raw_retention_us: policy.retention_us,
            aggregate_retention_us: policy.aggregate_retention_us,
            raw_available_from_us,
            aggregate_available_from_us,
            corrections_available_from_us: raw_available_from_us,
            available_to_us,
            aggregate_resolutions: policy.retained_aggregate_resolutions,
        })
    }

    /// Return retention boundaries for a registered numeric series key.
    ///
    /// # Errors
    ///
    /// Returns an error for an unregistered series or unavailable storage.
    pub fn series_info(&self, series: SeriesKey) -> Result<SeriesStorageInfo> {
        self.ensure_available()?;
        let policy_id = self.inner.state.read()?.catalog.policy_for(series)?.id;
        Ok(SeriesStorageInfo {
            series,
            retention: self.retention_info(policy_id)?,
        })
    }

    /// Create a space through atomic manifest publication.
    ///
    /// # Errors
    ///
    /// Returns name, conflict, I/O, or stopped-publisher errors.
    pub fn create_space(&self, name: impl Into<String>) -> Result<SpaceDefinition> {
        self.ensure_writable()?;
        let name = name.into();
        validate_name(&name, "space")?;
        let (reply_tx, reply_rx) = bounded(1);
        self.request_command(
            &self.inner.publish_tx,
            PublishCommand::CreateSpace {
                name,
                created_at_us: self.inner.options.clock.now_us(),
                reply: reply_tx,
            },
            &reply_rx,
            OverloadReason::PublicationQueue,
            "create_space",
        )
    }

    /// Create an immutable policy through atomic manifest publication.
    ///
    /// # Errors
    ///
    /// Returns parameter, conflict, I/O, or stopped-publisher errors.
    pub fn create_policy(&self, options: StoragePolicyOptions) -> Result<StoragePolicy> {
        self.ensure_writable()?;
        options.validate()?;
        let (reply_tx, reply_rx) = bounded(1);
        self.request_command(
            &self.inner.publish_tx,
            PublishCommand::CreatePolicy {
                options,
                reply: reply_tx,
            },
            &reply_rx,
            OverloadReason::PublicationQueue,
            "create_policy",
        )
    }

    /// Create a bucket in an existing space with the selected policy.
    ///
    /// # Errors
    ///
    /// Returns name, missing-reference, conflict, I/O, or publisher errors.
    pub fn create_bucket(
        &self,
        space: impl Into<String>,
        name: impl Into<String>,
        policy: impl Into<PolicySelector>,
    ) -> Result<BucketDefinition> {
        self.ensure_writable()?;
        let space = space.into();
        let name = name.into();
        validate_name(&space, "space")?;
        validate_name(&name, "bucket")?;
        let (reply_tx, reply_rx) = bounded(1);
        self.request_command(
            &self.inner.write_tx,
            WriteCommand::CreateBucket {
                space,
                name,
                policy: policy.into(),
                created_at_us: self.inner.options.clock.now_us(),
                reply: reply_tx,
            },
            &reply_rx,
            OverloadReason::WriteQueue,
            "create_bucket",
        )
    }

    /// Resolve a logical series name using the current catalog snapshot.
    ///
    /// # Errors
    ///
    /// Returns closed-storage or missing-catalog-entry errors.
    pub fn resolve_series(&self, series: &SeriesRef) -> Result<(SeriesKey, StoragePolicy)> {
        self.catalog()?.resolve(series)
    }
}

pub(super) fn storage_domain(
    catalog: &CatalogSnapshot,
    series: SeriesKey,
    timestamp_us: i64,
    virtual_shards: u16,
    fallback_partition_span_us: i64,
) -> Result<Domain> {
    let (policy_id, partition_span_us) = catalog
        .storage_layout(series)
        .unwrap_or((1, fallback_partition_span_us));
    Domain::for_point(
        series,
        timestamp_us,
        virtual_shards,
        policy_id,
        partition_span_us,
    )
}

pub(super) fn validate_registered_write_times(
    inner: &Inner,
    writes: &[Point],
    observed_now_us: i64,
) -> Result<()> {
    let mut state = inner.state.write()?;
    state.maintenance_now_us = state.maintenance_now_us.max(observed_now_us);
    let now_us = state.maintenance_now_us;
    let catalog = state.catalog.clone();
    for operation in writes {
        if let Some(policy) = catalog.registered_policy(operation.series) {
            validate_write_time(&mut state, policy, operation.timestamp_us, now_us)?;
        }
    }
    Ok(())
}

pub(super) fn validate_strict_writes(
    writes: &[Point],
    watermarks: &HashMap<SeriesKey, i64>,
) -> Result<Vec<(SeriesKey, i64)>> {
    let mut updates = HashMap::<SeriesKey, i64>::new();
    for operation in writes {
        let series = operation.series;
        let timestamp_us = operation.timestamp_us;
        match updates.entry(series) {
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                if timestamp_us <= *entry.get() {
                    return Err(invalid(
                        "strictly increasing series requires timestamp greater than its previous timestamp",
                    ));
                }
                entry.insert(timestamp_us);
            }
            std::collections::hash_map::Entry::Vacant(entry) => {
                if watermarks
                    .get(&series)
                    .is_some_and(|previous| timestamp_us <= *previous)
                {
                    return Err(invalid(
                        "strictly increasing series requires timestamp greater than its previous timestamp",
                    ));
                }
                entry.insert(timestamp_us);
            }
        }
    }
    Ok(updates.into_iter().collect())
}

pub(super) fn ensure_series_namespace_unused(
    inner: &Inner,
    space_id: u32,
    bucket_id: u32,
) -> Result<()> {
    let (batches, segments) = {
        let state = inner.state.read()?;
        let mut batches = state.active_batches.clone();
        batches.extend(
            state
                .frozen
                .iter()
                .flat_map(|generation| generation.batches.iter().cloned()),
        );
        (batches, state.files.clone())
    };
    if batches.iter().any(|batch| {
        batch.iter().any(|point| {
            point.key.series.space_id == space_id && point.key.series.bucket_id == bucket_id
        })
    }) {
        return Err(Error::Catalog(format!(
            "numeric series namespace already contains data: {space_id}/{bucket_id}"
        )));
    }
    segments.all(|entry| {
        let segment = segments.resources.open(entry.descriptor, None)?;
        if segment.contains_series_namespace(space_id, bucket_id)? {
            return Err(Error::Catalog(format!(
                "numeric series namespace already contains data: {space_id}/{bucket_id}"
            )));
        }
        Ok(())
    })
}

pub(super) fn compression_profile_for_domain(
    catalog: &CatalogSnapshot,
    domain: Domain,
    now_us: i64,
    level: u8,
) -> CompressionProfile {
    let Some(policy) = catalog.policy_by_id(domain.policy_id) else {
        return if level == 0 {
            CompressionProfile::hot_fast()
        } else {
            CompressionProfile::columnar()
        };
    };
    let hot_cutoff = i128::from(now_us) - i128::from(policy.hot_span_us);
    if i128::from(domain.window_end_us) <= hot_cutoff {
        policy.cold_compression
    } else {
        policy.hot_compression
    }
}

pub(super) fn sealed_compression_profile(
    catalog: &CatalogSnapshot,
    policy_id: u32,
) -> CompressionProfile {
    catalog
        .policy_by_id(policy_id)
        .map_or_else(CompressionProfile::columnar, |policy| {
            policy.cold_compression
        })
}

pub(super) fn validate_write_time(
    state: &mut State,
    policy: &StoragePolicy,
    timestamp_us: i64,
    now_us: i64,
) -> Result<()> {
    if let Some(max_future_skew_us) = policy.max_future_skew_us
        && i128::from(timestamp_us) > i128::from(now_us) + i128::from(max_future_skew_us)
    {
        return Err(invalid("point exceeds the maximum future skew"));
    }
    if let Some(cutoff) = advance_retention_cutoff(state, policy, now_us)
        && timestamp_us < cutoff
    {
        return Err(Error::ExpiredPoint(
            "point is outside the retention interval".into(),
        ));
    }
    if let Some(lateness_us) = policy.max_lateness_us
        && i128::from(timestamp_us) < i128::from(now_us) - i128::from(lateness_us)
    {
        return Err(Error::ExpiredPoint(
            "point exceeds the maximum lateness interval".into(),
        ));
    }
    Ok(())
}

pub(super) fn advance_retention_cutoff(
    state: &mut State,
    policy: &StoragePolicy,
    now_us: i64,
) -> Option<i64> {
    advance_aggregate_retention_cutoff(state, policy, now_us);
    let retention_us = policy.retention_us?;
    let calculated = i128::from(now_us) - i128::from(retention_us);
    let calculated = i64::try_from(calculated).ok().map(|cutoff| {
        if policy.aggregate_retention_us.is_some() {
            cutoff.div_euclid(DAY_US) * DAY_US
        } else {
            cutoff
        }
    });
    let persisted = state.retention_cutoffs.get(&policy.id).copied();
    let cutoff = match (calculated, persisted) {
        (Some(calculated), Some(persisted)) => calculated.max(persisted),
        (Some(calculated), None) => calculated,
        (None, persisted) => return persisted,
    };
    state.retention_cutoffs.insert(policy.id, cutoff);
    Some(cutoff)
}

pub(super) fn advance_aggregate_retention_cutoff(
    state: &mut State,
    policy: &StoragePolicy,
    now_us: i64,
) -> Option<i64> {
    let retention_us = policy.aggregate_retention_us?;
    let calculated = i128::from(now_us) - i128::from(retention_us);
    let calculated = i64::try_from(calculated)
        .ok()
        .map(|cutoff| cutoff.div_euclid(DAY_US) * DAY_US);
    let persisted = state.aggregate_retention_cutoffs.get(&policy.id).copied();
    let cutoff = match (calculated, persisted) {
        (Some(calculated), Some(persisted)) => calculated.max(persisted),
        (Some(calculated), None) => calculated,
        (None, persisted) => return persisted,
    };
    state.aggregate_retention_cutoffs.insert(policy.id, cutoff);
    Some(cutoff)
}
