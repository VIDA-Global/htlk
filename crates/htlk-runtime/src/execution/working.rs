//! Bounded scheduling metadata retained only by the active coordinator.
use crate::store::{RecordFamily, RecordKey, RecordWrite, StoredRecord};
use htlk_executable::cbor::{self, Limits};
use std::{
    cell::RefCell,
    collections::{BTreeMap, BTreeSet},
};

pub(super) type Slot = Option<WorkingSet>;
tokio::task_local! { pub(super) static WORKING: RefCell<Slot>; }

#[derive(Default)]
pub(super) struct WorkingSet {
    pub sequence: i64,
    pub scopes_complete: bool,
    records: BTreeMap<RecordKey, (StoredRecord, usize)>,
    bytes: usize,
    waiting: BTreeSet<String>,
    dependents: BTreeMap<String, Vec<String>>,
}

impl WorkingSet {
    pub fn reset(&mut self, sequence: i64) {
        *self = Self {
            sequence,
            ..Self::default()
        };
    }

    pub fn get(&self, key: &RecordKey) -> Option<StoredRecord> {
        self.records.get(key).map(|(record, _)| record.clone())
    }

    pub fn waiting(&self, id: &str) -> bool {
        self.waiting.contains(id)
    }

    pub fn defer(&mut self, dependent: &str, producer: &str) {
        if self.waiting.len() < 4096 && self.waiting.insert(dependent.to_owned()) {
            self.dependents
                .entry(producer.to_owned())
                .or_default()
                .push(dependent.to_owned());
        }
    }

    pub fn scopes(&self) -> Vec<StoredRecord> {
        self.records
            .values()
            .filter(|(r, _)| r.key.family == RecordFamily::Scope)
            .map(|(r, _)| r.clone())
            .collect()
    }

    pub fn retain(&mut self, record: StoredRecord, limits: &Limits) -> bool {
        if self.records.len() >= 4096 && !self.records.contains_key(&record.key) {
            return false;
        }
        let key = record.key.clone();
        if let Some((_, old_size)) = self.records.remove(&key) {
            self.bytes -= old_size;
        }
        let Ok(encoded) = cbor::encode(&record.payload, limits) else {
            return false;
        };
        let size = encoded
            .len()
            .saturating_add(key.id.len())
            .saturating_add(64);
        if self.bytes.saturating_add(size) <= limits.max_document_bytes.min(1024 * 1024) {
            self.bytes += size;
            self.records.insert(key, (record, size));
            true
        } else {
            false
        }
    }

    pub fn committed(&mut self, sequence: i64, writes: Vec<RecordWrite>, limits: &Limits) {
        if self.sequence.checked_add(1) != Some(sequence) {
            self.reset(0);
            return;
        }
        self.sequence = sequence;
        for write in writes {
            if !matches!(
                write.key.family,
                RecordFamily::Invocation | RecordFamily::Guard | RecordFamily::Scope
            ) {
                continue;
            }
            let Some(revision) = write.expected_revision.unwrap_or(0).checked_add(1) else {
                self.reset(0);
                return;
            };
            let record = StoredRecord {
                key: write.key,
                revision,
                final_record: write.final_record,
                payload: write.payload,
            };
            if record.key.family == RecordFamily::Invocation
                && let Some(dependents) = self.dependents.remove(&record.key.id)
            {
                for dependent in dependents {
                    self.waiting.remove(&dependent);
                }
            }
            let scope = record.key.family == RecordFamily::Scope;
            if !self.retain(record, limits) && scope {
                self.scopes_complete = false;
            }
        }
    }
}

pub(super) fn with<R>(f: impl FnOnce(&mut WorkingSet) -> R) -> Option<R> {
    WORKING
        .try_with(|cell| cell.borrow_mut().as_mut().map(f))
        .ok()
        .flatten()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn successive_commits_wake_only_dependents_and_gap_discards_all_hints() {
        let limits = Limits::default();
        let mut cache = WorkingSet::default();
        cache.reset(6);
        cache.defer("consumer", "producer");
        cache.defer("unrelated", "other");
        let key = RecordKey {
            family: RecordFamily::Invocation,
            id: "producer".into(),
        };
        cache.committed(
            7,
            vec![RecordWrite {
                key: key.clone(),
                expected_revision: Some(2),
                final_record: true,
                payload: cbor::Value::Null,
            }],
            &limits,
        );
        assert_eq!(cache.get(&key).unwrap().revision, 3);
        assert!(!cache.waiting("consumer"));
        assert!(cache.waiting("unrelated"));
        cache.committed(9, vec![], &limits);
        assert!(cache.get(&key).is_none());
        assert!(!cache.waiting("unrelated"));
    }
}
