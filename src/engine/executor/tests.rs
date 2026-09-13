//! Worker accounting at queue/reply boundaries, including abandoned queries.
use super::*;
use crate::admission::WorkMemoryGovernor;
use crate::query::QueryOptions;
use crate::types::{DAY_US, Durability, GroupRequest, Point};
use crate::{OpenOptions, Storage};
use std::time::Duration;
use tempfile::TempDir;

fn budget() -> Result<Arc<QueryBudget>> {
    QueryBudget::new_with_admission(
        QueryOptions::default(),
        WorkMemoryGovernor::new(1024 * 1024),
    )
}

fn lease() -> QueryLease {
    QueryLease {
        _guard: None,
        _storage_lock: None,
    }
}

#[test]
fn planning_metrics_cover_wait_work_cancel_panic_and_abandoned_reply() -> Result<()> {
    // No sleeps: channels establish the queue wait and active-work boundaries.
    for outcome in 0..4 {
        let (sender, receiver) = bounded(1);
        let worker = thread::spawn(move || query_worker_loop(&receiver));
        let (entered, entries) = bounded(1);
        let (release, releases) = bounded(1);
        sender
            .send(QueryWork::Pause {
                entered,
                release: releases,
            })
            .expect("pause worker");
        entries
            .recv_timeout(Duration::from_secs(2))
            .expect("worker paused");
        let budget = budget()?;
        let worker_budget = Arc::clone(&budget);
        let (started, starts) = bounded(1);
        let (finish, finishes) = bounded::<()>(1);
        let (reply, replies) = bounded(1);
        let enqueued_at = Instant::now();
        sender
            .send(QueryWork::Planning {
                lease: lease(),
                index: 0,
                enqueued_at,
                query_budget: Arc::clone(&budget),
                reply,
                task: QueryPlanningTask::Probe(Box::new(move || {
                    started.send(Instant::now()).expect("entered planning");
                    let _ = finishes.recv();
                    assert_ne!(outcome, 2, "injected planning panic");
                    worker_budget.check()
                })),
            })
            .expect("queued planning");
        let waiting_until = Instant::now();
        assert_eq!(budget.stats().executor_planning_tasks, 0);
        drop(release);
        let active_since = starts
            .recv_timeout(Duration::from_secs(2))
            .expect("planning started");
        assert_eq!(budget.stats().executor_planning_tasks, 0);
        if outcome == 1 {
            budget.cancel();
        }
        let replies = if outcome == 3 {
            drop(replies);
            None
        } else {
            Some(replies)
        };
        let working_until = Instant::now();
        drop(finish);
        if let Some(replies) = replies {
            let (_, result) = replies
                .recv_timeout(Duration::from_secs(2))
                .expect("planning reply");
            match outcome {
                0 => assert!(result.is_ok()),
                1 => assert!(matches!(result, Err(Error::QueryCancelled))),
                2 => assert!(matches!(result, Err(Error::WorkerStopped))),
                _ => unreachable!(),
            }
            // The reply must never race ahead of the accounting.
            assert_eq!(budget.stats().executor_planning_tasks, 1);
        }
        let (entered, entries) = bounded(1);
        let (release, releases) = bounded(1);
        sender
            .send(QueryWork::Pause {
                entered,
                release: releases,
            })
            .expect("completion barrier");
        entries
            .recv_timeout(Duration::from_secs(2))
            .expect("worker survived");
        let stats = budget.stats();
        assert_eq!(stats.executor_planning_tasks, 1);
        assert_eq!(stats.executor_series_tasks, 0);
        assert!(
            stats.executor_planning_queue_duration >= waiting_until.duration_since(enqueued_at)
        );
        assert!(
            stats.executor_planning_active_duration >= working_until.duration_since(active_since)
        );
        assert_eq!(
            stats.executor_queue_duration,
            stats.executor_planning_queue_duration
        );
        assert_eq!(
            stats.executor_active_duration,
            stats.executor_planning_active_duration
        );
        drop(release);
        drop(sender);
        worker.join().expect("worker exit");
    }
    Ok(())
}

#[test]
fn rejected_planning_submission_does_not_count_as_completed_work() -> Result<()> {
    for disconnected in [false, true] {
        let (sender, receiver) = bounded(1);
        let (entered, _) = bounded(1);
        let (_release, releases) = bounded(1);
        sender
            .send(QueryWork::Pause {
                entered,
                release: releases,
            })
            .expect("full queue");
        let _receiver = if disconnected {
            drop(receiver);
            None
        } else {
            Some(receiver)
        };
        let executor = QueryExecutor {
            sender,
            worker_count: 1,
            _workers: Vec::new(),
        };
        let budget = QueryBudget::new_with_admission(
            QueryOptions {
                deadline: Some(Instant::now() + Duration::from_millis(20)),
                ..QueryOptions::default()
            },
            WorkMemoryGovernor::new(1024),
        )?;
        let result = executor.plan(
            vec![QueryPlanningTask::Probe(Box::new(|| {
                panic!("rejected task ran")
            }))],
            lease(),
            &budget,
        );
        if disconnected {
            assert!(matches!(result, Err(Error::WorkerStopped)));
        } else {
            assert!(matches!(
                result,
                Err(Error::Overloaded(OverloadReason::QueryQueue))
            ));
        }
        let stats = budget.stats();
        assert_eq!(stats.executor_planning_tasks, 0);
        assert_eq!(stats.executor_queue_duration, Duration::ZERO);
        assert_eq!(stats.executor_active_duration, Duration::ZERO);
    }
    Ok(())
}

#[test]
fn completed_group_reports_planning_and_series_without_cross_query_accumulation() -> Result<()> {
    let directory = TempDir::new()?;
    let storage = Storage::open(
        directory.path(),
        OpenOptions {
            virtual_shards: 4,
            history_seal_lag: None,
            ..OpenOptions::default()
        },
    )?;
    let series = (0..300)
        .map(|key| SeriesKey::new(1, 1, key))
        .collect::<Vec<_>>();
    let points = series
        .iter()
        .map(|key| Point::new(*key, 1, key.key as f64))
        .collect::<Result<Vec<_>>>()?;
    storage.write_batch(points, Durability::Sync)?;
    storage.checkpoint()?;
    for _ in 0..2 {
        let result = storage.group(GroupRequest::new(series.clone(), 0, DAY_US)?)?;
        assert_eq!(result.rows.len(), 300);
        assert_eq!(result.rows.iter().map(|row| row.count).sum::<u64>(), 300);
        assert_eq!(result.rows.iter().map(|row| row.sum).sum::<f64>(), 44_850.0);
        let stats = result.stats;
        if storage.inner.query_executor.is_parallel() {
            assert!(stats.executor_planning_tasks > 0);
            assert_eq!(stats.executor_series_tasks, 300);
        } else {
            assert_eq!(stats.executor_planning_tasks, 0);
            assert_eq!(stats.executor_series_tasks, 0);
        }
        assert_eq!(
            stats.executor_queue_duration,
            stats.executor_planning_queue_duration + stats.executor_series_queue_duration
        );
        assert_eq!(
            stats.executor_active_duration,
            stats.executor_planning_active_duration + stats.executor_series_active_duration
        );
    }
    let single = storage.group(GroupRequest::new(vec![series[0]], 0, DAY_US)?)?;
    assert_eq!(single.stats.executor_planning_tasks, 0);
    assert_eq!(single.stats.executor_series_tasks, 0);
    storage.close()
}
