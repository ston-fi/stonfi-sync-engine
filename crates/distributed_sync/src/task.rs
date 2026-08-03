use crate::traits::TaskPayload;
use bincode::{Decode, Encode};
use stonfi_sync_core::errors::{SyncCoreError, SyncCoreResult};
use stonfi_sync_core::sync_engine::SyncHeight;

/// A serializable inclusive height range.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct RangeTask {
    /// First height in the inclusive range.
    pub from: SyncHeight,
    /// Last height in the inclusive range.
    pub to: SyncHeight,
}

impl TaskPayload for RangeTask {
    fn encode(&self) -> SyncCoreResult<Vec<u8>> {
        bincode::encode_to_vec(self, bincode::config::standard()).map_err(SyncCoreError::custom)
    }

    fn decode(data: &[u8]) -> SyncCoreResult<Self> {
        decode_exact(data)
    }
}

/// Empty result for tasks whose successful completion carries no data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Encode, Decode)]
pub struct EmptyTaskResult;

impl TaskPayload for EmptyTaskResult {
    fn encode(&self) -> SyncCoreResult<Vec<u8>> {
        bincode::encode_to_vec(self, bincode::config::standard()).map_err(SyncCoreError::custom)
    }

    fn decode(data: &[u8]) -> SyncCoreResult<Self> {
        decode_exact(data)
    }
}

fn decode_exact<T: Decode<()>>(data: &[u8]) -> SyncCoreResult<T> {
    let (value, consumed) =
        bincode::decode_from_slice(data, bincode::config::standard()).map_err(SyncCoreError::invalid_args)?;
    if consumed != data.len() {
        return Err(SyncCoreError::invalid_args(format!(
            "task payload contains {} trailing bytes",
            data.len() - consumed
        )));
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::{EmptyTaskResult, RangeTask};
    use crate::traits::TaskPayload;
    use stonfi_sync_core::sync_engine::SyncHeight;

    #[test]
    fn test_range_task_round_trip_and_trailing_bytes() -> anyhow::Result<()> {
        let from = SyncHeight::from(u32::MAX) + 1;
        let task = RangeTask { from, to: from + 10 };
        let encoded = task.encode()?;

        assert_eq!(RangeTask::decode(&encoded)?, task);

        let mut with_trailing = encoded;
        with_trailing.push(0);
        assert!(RangeTask::decode(&with_trailing).is_err());
        Ok(())
    }

    #[test]
    fn test_empty_result_rejects_malformed_payload() {
        assert!(EmptyTaskResult::decode(&[1]).is_err());
    }
}
