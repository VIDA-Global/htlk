//! Host-selected partitions of independent durable stores.
use super::{Store, StoreError, StorePosition};
use crate::identity::RunId;
use htlk_executable::Identifier;
use std::collections::{BTreeMap, BTreeSet};

/// A bounded, immutable directory of independent HTLK stores.
///
/// Hosts assign each atomic workload (including its idempotency scope and
/// artifact references) to one partition. This directory neither splits a
/// transaction nor grants admission, discovery, or value access.
pub struct StorePartitions {
    stores: BTreeMap<Identifier, Store>,
    namespaces: BTreeMap<String, Identifier>,
}

/// A set of independently committed history positions, keyed by partition.
/// No cross-partition commit order or atomic cross-store snapshot is implied.
pub type PartitionPositions = BTreeMap<Identifier, StorePosition>;

impl StorePartitions {
    /// Checks the namespaces of 1–256 host-selected partitions. Hosts must keep
    /// routing assignments stable across restarts and idempotent retries.
    ///
    /// # Errors
    /// Returns invalid count/duplicate namespace or an underlying store error.
    pub async fn new(stores: BTreeMap<Identifier, Store>) -> Result<Self, StoreError> {
        if stores.is_empty() || stores.len() > 256 {
            return Err(StoreError::Invalid);
        }
        let mut namespaces = BTreeMap::new();
        for (partition, store) in &stores {
            let position = store.inspection_position().await?;
            if namespaces
                .insert(position.namespace().to_owned(), partition.clone())
                .is_some()
            {
                return Err(StoreError::Invalid);
            }
        }
        Ok(Self { stores, namespaces })
    }

    /// Selects the exact trusted partition for registration, run start, an
    /// idempotent retry, or non-run-owned state. No fallback is attempted.
    ///
    /// # Errors
    /// Returns missing for an unconfigured partition.
    pub fn store(&self, partition: &Identifier) -> Result<&Store, StoreError> {
        self.stores.get(partition).ok_or(StoreError::Missing)
    }

    /// Selects the home partition of a runtime-generated run ID. Run lookup and
    /// authorization still belong to the selected runtime. Hosts using opaque
    /// custom store-level IDs must retain their partition alongside the ID.
    ///
    /// # Errors
    /// Returns invalid for other ID forms or missing for an unknown namespace.
    pub fn partition_for_run(&self, run: &RunId) -> Result<&Identifier, StoreError> {
        let (namespace, number) = run
            .as_str()
            .strip_prefix("run:")
            .and_then(|tail| tail.split_once(':'))
            .ok_or(StoreError::Invalid)?;
        if !number.parse::<i64>().is_ok_and(|number| number > 0)
            || !number.bytes().all(|byte| byte.is_ascii_digit())
        {
            return Err(StoreError::Invalid);
        }
        self.namespaces.get(namespace).ok_or(StoreError::Missing)
    }

    /// Captures one committed position per store. Reuse the per-partition value
    /// for paged reads rather than comparing positions from different stores.
    ///
    /// # Errors
    /// Returns an underlying read error; never returns a partial vector.
    pub async fn positions(&self) -> Result<PartitionPositions, StoreError> {
        let mut positions = BTreeMap::new();
        let mut seen = BTreeSet::new();
        for (partition, store) in &self.stores {
            let position = store.inspection_position().await?;
            if !seen.insert(position.namespace().to_owned())
                || self.namespaces.get(position.namespace()) != Some(partition)
            {
                return Err(StoreError::Conflict);
            }
            positions.insert(partition.clone(), position);
        }
        Ok(positions)
    }
}
