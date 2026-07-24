use crate::errors::{SyncCoreError, SyncCoreResult};
use crate::sync_engine::SyncHeight;
use futures::{StreamExt, stream::FuturesUnordered};
use tokio::sync::watch::{Receiver, Sender};

pub(super) type SyncSender = Sender<SyncHeight>;
/// Receives the latest completed height from an initiator or synchronizer.
pub type SyncReceiver = Receiver<SyncHeight>;

/// Waits for the minimum usable height from all configured sources.
pub(super) struct MultiReceiver {
    receivers: Vec<SyncReceiver>,
}

impl MultiReceiver {
    // We don't expect much here
    pub(super) fn new(receivers: Vec<SyncReceiver>) -> SyncCoreResult<Self> {
        if receivers.is_empty() {
            return Err(SyncCoreError::invalid_args("receivers can't be empty"));
        }
        Ok(Self { receivers })
    }

    pub(super) async fn wait_after(&mut self, after: SyncHeight) -> Option<SyncHeight> {
        let mut cur_values: Vec<SyncHeight> = self.receivers.iter().map(|receiver| *receiver.borrow()).collect();

        let new_height = loop {
            if self.receivers.iter().any(|rcv| rcv.has_changed().is_err()) {
                return None;
            }
            let Some(min_height) = cur_values.iter().copied().min() else {
                log::warn!("[MULTI_RECEIVER] no receivers available while waiting for progress");
                return None;
            };
            if min_height > after {
                break min_height;
            }

            // Collect only receivers that still block progress.
            let mut futs: FuturesUnordered<_> = self
                .receivers
                .iter_mut()
                .enumerate()
                .filter_map(|(pos, rcv)| {
                    (cur_values[pos] <= after)
                        .then_some(async move { rcv.changed().await.ok().map(|_| (pos, *rcv.borrow())) })
                })
                .collect();

            match futs.next().await {
                Some(Some((idx, val))) => cur_values[idx] = val,
                Some(None) => return None, // receiver closed
                None => {
                    log::warn!("[MULTI_RECEIVER] no blocking receivers left while progress is still pending");
                    return None;
                },
            }
        };
        Some(new_height)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::watch;

    #[tokio::test]
    async fn changed_works() -> anyhow::Result<()> {
        let (tx1, rx1) = watch::channel(15);
        let (tx2, rx2) = watch::channel(20);
        let mut multi_receiver = MultiReceiver::new(vec![rx1, rx2])?;
        assert_eq!(Some(15), multi_receiver.wait_after(14).await);
        tx2.send(30)?;
        assert_eq!(Some(15), multi_receiver.wait_after(14).await);
        tx1.send(25)?;
        assert_eq!(Some(25), multi_receiver.wait_after(15).await);
        tx1.send(35)?;
        assert_eq!(Some(30), multi_receiver.wait_after(25).await);
        Ok(())
    }

    #[tokio::test]
    async fn changed_ignores_receivers_already_above_after() -> anyhow::Result<()> {
        let (tx1, rx1) = watch::channel(100);
        let (tx2, rx2) = watch::channel(1);
        let (tx3, rx3) = watch::channel(1);
        let mut multi_receiver = MultiReceiver::new(vec![rx1, rx2, rx3])?;

        let task = tokio::spawn(async move { multi_receiver.wait_after(1).await });
        tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;
        tx1.send(101)?;
        tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;
        tx2.send(2)?;
        tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;
        tx3.send(2)?;

        assert_eq!(Some(2), task.await?);
        Ok(())
    }

    #[tokio::test]
    async fn changed_parent_does_not_rewind_an_active_wait() -> anyhow::Result<()> {
        let (wrapped_tx, wrapped_rx) = watch::channel(20);
        let (blocking_tx, blocking_rx) = watch::channel(10);
        let mut multi_receiver = MultiReceiver::new(vec![wrapped_rx, blocking_rx])?;

        {
            let current_wait = multi_receiver.wait_after(10);
            futures::pin_mut!(current_wait);
            assert!(futures::poll!(&mut current_wait).is_pending());

            wrapped_tx.send(0)?;
            blocking_tx.send(11)?;
            assert_eq!(Some(11), current_wait.await);
        }

        {
            let next_wait = multi_receiver.wait_after(11);
            futures::pin_mut!(next_wait);
            assert!(futures::poll!(&mut next_wait).is_pending());

            blocking_tx.send(12)?;
            assert!(futures::poll!(&mut next_wait).is_pending());
            wrapped_tx.send(12)?;
            assert_eq!(Some(12), next_wait.await);
        }
        Ok(())
    }

    #[tokio::test]
    async fn changed_returns_none_when_receiver_closed() -> anyhow::Result<()> {
        let (tx, rx) = watch::channel(0);
        drop(tx);
        let mut multi_receiver = MultiReceiver::new(vec![rx])?;
        assert_eq!(None, multi_receiver.wait_after(0).await);
        Ok(())
    }

    #[tokio::test]
    async fn changed_returns_none_when_all_blocking_receivers_close() -> anyhow::Result<()> {
        let (tx1, rx1) = watch::channel(0);
        let (_tx2, rx2) = watch::channel(10);
        let mut multi_receiver = MultiReceiver::new(vec![rx1, rx2])?;

        let task = tokio::spawn(async move { multi_receiver.wait_after(0).await });
        tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;
        drop(tx1);

        assert_eq!(None, task.await?);
        Ok(())
    }
}
