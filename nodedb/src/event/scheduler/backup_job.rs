// SPDX-License-Identifier: BUSL-1.1

//! Scheduled logical backups: the backup job kind of the cron scheduler.
//!
//! A SQL schedule cannot run a backup. Its body runs as one transaction
//! (`job_run::execute_job`), and `BACKUP DATABASE` refuses to run inside
//! one: its object write is outside the transaction and no rollback undoes
//! it. So each `[[backup.schedule]]` entry is its own job, fired through the
//! same bounded dispatcher and recorded in the same job history.
//!
//! Only the cluster's system coordinator, the leader of vShard 0, fires
//! backups. It picks the due minute from the replicated schedule mark
//! (`control::backup::schedule::marks`), read after this node applied the
//! metadata group through a confirmed read index. A new coordinator
//! therefore runs any due minute the previous one did not finish, and never
//! one it did. The envelope is named for the scheduled minute, so a minute
//! run twice writes the same object.
//!
//! Every blocking step of a job (catalog reads and writes, metadata
//! proposals, job history, audit) runs on the blocking pool.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

use tracing::{info, warn};

use super::backup_due::{BackupStep, next_step};
use super::coordinator::is_system_coordinator;
use super::cron::CronExpr;
use super::dispatcher::{DispatchOutcome, JobDispatcher};
use super::history::JobHistoryStore;
use super::types::JobRun;
use crate::config::server::BackupScheduleSettings;
use crate::control::backup::schedule::blocking::off_runtime;
use crate::control::backup::schedule::{BackupRun, marks, run_scheduled_backup};
use crate::control::metrics::SystemMetrics;
use crate::control::state::SharedState;

/// Seconds a job waits after a failed step before it tries again.
const RETRY_AFTER_SECS: u64 = 60;

/// One configured backup schedule.
#[derive(Clone)]
struct BackupJob {
    schedule: BackupScheduleSettings,
    cron: CronExpr,
    name: String,
}

/// Per-job state on this node.
#[derive(Default)]
struct JobSlots {
    running: HashSet<usize>,
    retry_at_secs: HashMap<usize, u64>,
    /// The highest mark this node read linearizably or raised, per job. The
    /// replicated mark only rises, so this is a lower bound of it: a step
    /// that is idle under it is idle under the replicated mark too. It only
    /// lets an idle tick skip the read-index round. A due step is always
    /// confirmed against a fresh linearizable read.
    observed: HashMap<usize, u64>,
    /// Ticks skipped in a row because the mark read cannot be confirmed.
    skipped: HashMap<usize, u64>,
}

/// Every backup job of this node.
///
/// Nothing here can suppress a due run: in-flight work and the retry delay
/// are this node's own, and dropped when it stops being the coordinator,
/// and `observed` never exceeds the replicated mark.
pub struct BackupJobs {
    jobs: Vec<BackupJob>,
    slots: Arc<Mutex<JobSlots>>,
}

impl BackupJobs {
    /// Jobs for `schedules`. Config load refuses a bad cron, so a schedule
    /// whose cron does not parse here is logged and skipped.
    pub fn new(schedules: &[BackupScheduleSettings]) -> Self {
        let jobs = schedules
            .iter()
            .filter_map(|schedule| match CronExpr::parse(&schedule.cron) {
                Ok(cron) => Some(BackupJob {
                    name: schedule.job_name(),
                    schedule: schedule.clone(),
                    cron,
                }),
                Err(e) => {
                    warn!(
                        database = %schedule.database,
                        cron = %schedule.cron,
                        error = %e,
                        "backup schedule skipped: invalid cron"
                    );
                    None
                }
            })
            .collect();
        Self {
            jobs,
            slots: Arc::new(Mutex::new(JobSlots::default())),
        }
    }

    /// Jobs this node has in flight.
    pub fn in_flight(&self) -> usize {
        self.lock().running.len()
    }

    /// Dispatch a tick job for every schedule that can be due, if this node
    /// is the system coordinator. `now_secs` is the scheduler clock. The job
    /// reads the mark linearizably and decides; this call does no I/O.
    pub fn fire(
        &self,
        state: &Arc<SharedState>,
        dispatcher: &JobDispatcher,
        history: &Arc<JobHistoryStore>,
        now_secs: u64,
    ) {
        if self.jobs.is_empty() {
            return;
        }
        if !is_system_coordinator(state) {
            self.lock().retry_at_secs.clear();
            return;
        }
        let tz_offset = state.scheduler_config.cron_timezone.offset_seconds();
        let now_min = now_secs / 60;
        for (index, job) in self.jobs.iter().enumerate() {
            {
                let mut slots = self.lock();
                let waiting = slots
                    .retry_at_secs
                    .get(&index)
                    .is_some_and(|&at| now_secs < at);
                let lower_bound = slots.observed.get(&index).copied();
                let idle = lower_bound.is_some()
                    && next_step(lower_bound, now_min, &job.cron, tz_offset) == BackupStep::Idle;
                if waiting || idle || !slots.running.insert(index) {
                    continue;
                }
            }
            let tick = Tick {
                state: Arc::clone(state),
                history: Arc::clone(history),
                job: job.clone(),
                slots: Arc::clone(&self.slots),
                index,
                now_min,
                tz_offset,
            };
            let outcome = dispatcher.try_spawn(move |mut shutdown| async move {
                let result = tokio::select! {
                    result = tick.run() => result,
                    _ = shutdown.changed() => Err(crate::Error::Dispatch {
                        detail: "scheduler shutdown".into(),
                    }),
                };
                let mut slots = tick.slots.lock().unwrap_or_else(|p| p.into_inner());
                slots.running.remove(&tick.index);
                if result.is_err() {
                    // Measured on the scheduler clock the tick fired at.
                    slots.retry_at_secs.insert(
                        tick.index,
                        tick.now_min
                            .saturating_mul(60)
                            .saturating_add(RETRY_AFTER_SECS),
                    );
                } else {
                    slots.retry_at_secs.remove(&tick.index);
                }
                result
            });
            if outcome == DispatchOutcome::OverBudget {
                self.lock().running.remove(&index);
                warn!(job = %job.name, "scheduled backup rejected: concurrency cap reached");
            }
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, JobSlots> {
        self.slots.lock().unwrap_or_else(|p| p.into_inner())
    }
}

/// One dispatched tick of one job.
struct Tick {
    state: Arc<SharedState>,
    history: Arc<JobHistoryStore>,
    job: BackupJob,
    slots: Arc<Mutex<JobSlots>>,
    index: usize,
    now_min: u64,
    tz_offset: i32,
}

impl Tick {
    /// Read the mark linearizably, then carry out the step it gives.
    ///
    /// A read that cannot be confirmed skips the tick: running on a stale
    /// mark repeats a finished minute. A run records its outcome in the
    /// job history and the metrics, then raises the mark. The mark rises
    /// only after the envelope is written, so a leader change before it
    /// leaves the minute due. The coordinator lease is checked again before
    /// the envelope write and before each mark raise.
    async fn run(&self) -> crate::Result<()> {
        let (state, schedule, name) = (&self.state, &self.job.schedule, &self.job.name);
        if !is_system_coordinator(state) {
            return Ok(());
        }
        let mark = match marks::settled_through_linearizable(state, schedule).await {
            Ok(mark) => {
                self.read_confirmed();
                mark
            }
            Err(e) => {
                self.read_refused(&e);
                return Ok(());
            }
        };
        if let Some(mark) = mark {
            self.observe(mark);
        }
        let minute = match next_step(mark, self.now_min, &self.job.cron, self.tz_offset) {
            BackupStep::Idle => return Ok(()),
            BackupStep::Arm(minute) => {
                info!(job = %name, minute, "arming backup schedule");
                marks::raise(state, schedule, minute).await?;
                self.observe(minute);
                return Ok(());
            }
            BackupStep::Run(minute) => minute,
        };
        info!(job = %name, minute, "firing scheduled backup");
        let started_ms = unix_now_ms();
        let result =
            match run_scheduled_backup(state, schedule, minute.saturating_mul(60_000)).await {
                Ok(run) => match marks::raise(state, schedule, minute).await {
                    Ok(()) => {
                        self.observe(minute);
                        Ok(run)
                    }
                    Err(e) => Err(crate::Error::Storage {
                        engine: "backup".into(),
                        detail: format!(
                            "scheduled backup wrote '{}', but its schedule mark did not rise; the \
                         minute runs again and rewrites the same envelope: {e}",
                            run.envelope_uri
                        ),
                    }),
                },
                Err(e) => Err(e),
            };
        let finished_ms = unix_now_ms();
        let (lookup_state, database) = (Arc::clone(state), schedule.database.clone());
        let database_id = off_runtime("scheduled backup database lookup", move || {
            Ok(lookup_state
                .credentials
                .catalog()
                .get_database_id_by_name(&database)
                .ok()
                .flatten()
                .map_or(0, |id| id.as_u64()))
        })
        .await?;
        let row = record_outcome(
            name,
            database_id,
            started_ms,
            finished_ms,
            &result,
            state.system_metrics.as_deref(),
        );
        let history = Arc::clone(&self.history);
        let job_name = name.clone();
        if let Err(e) = off_runtime("backup job history write", move || history.record(row)).await {
            warn!(job = %job_name, error = %e, "backup job history not recorded");
        }
        result.map(drop)
    }

    /// Count a skipped tick. The first skip of a run of them warns. Later
    /// ones only count, in the job's state and in
    /// `backup_schedule_ticks_skipped_total`.
    fn read_refused(&self, error: &crate::Error) {
        if let Some(metrics) = self.state.system_metrics.as_deref() {
            metrics
                .backup_schedule_ticks_skipped_total
                .fetch_add(1, Ordering::Relaxed);
        }
        let mut slots = self.slots.lock().unwrap_or_else(|p| p.into_inner());
        let skipped = slots.skipped.entry(self.index).or_insert(0);
        *skipped += 1;
        if *skipped == 1 {
            warn!(
                job = %self.job.name,
                error = %error,
                "backup schedule mark not confirmed; ticks are skipped until it is"
            );
        }
    }

    /// End a run of skipped ticks, and report how many it skipped.
    fn read_confirmed(&self) {
        let skipped = self
            .slots
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .skipped
            .remove(&self.index);
        if let Some(skipped) = skipped {
            warn!(
                job = %self.job.name,
                skipped,
                "backup schedule mark confirmed again after skipped ticks"
            );
        }
    }

    /// Raise the lower bound of the replicated mark this node holds.
    fn observe(&self, mark: u64) {
        let mut slots = self.slots.lock().unwrap_or_else(|p| p.into_inner());
        let seen = slots.observed.entry(self.index).or_insert(mark);
        *seen = (*seen).max(mark);
    }
}

/// The history row of one run, with its metrics recorded.
fn record_outcome(
    name: &str,
    database_id: u64,
    started_ms: u64,
    finished_ms: u64,
    result: &crate::Result<BackupRun>,
    metrics: Option<&SystemMetrics>,
) -> JobRun {
    let finished_secs = finished_ms / 1_000;
    let error = match result {
        Ok(run) => {
            info!(
                job = %name,
                envelope = %run.envelope_uri,
                bytes = run.bytes,
                deleted = run.deleted,
                "scheduled backup written"
            );
            if let Some(metrics) = metrics {
                metrics
                    .backup_schedule_runs_total
                    .fetch_add(1, Ordering::Relaxed);
                metrics
                    .backup_schedule_last_success_timestamp_seconds
                    .store(finished_secs, Ordering::Relaxed);
                metrics
                    .backup_schedule_envelopes_deleted_total
                    .fetch_add(run.deleted, Ordering::Relaxed);
            }
            None
        }
        Err(e) => {
            warn!(job = %name, error = %e, "scheduled backup failed");
            if let Some(metrics) = metrics {
                metrics
                    .backup_schedule_failures_total
                    .fetch_add(1, Ordering::Relaxed);
                metrics
                    .backup_schedule_last_failure_timestamp_seconds
                    .store(finished_secs, Ordering::Relaxed);
            }
            Some(e.to_string())
        }
    };
    JobRun {
        database_id,
        schedule_name: name.to_string(),
        tenant_id: 0,
        started_at: started_ms,
        duration_ms: finished_ms.saturating_sub(started_ms),
        success: error.is_none(),
        error,
    }
}

fn unix_now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_invalid_cron_is_skipped() {
        let schedule = |cron: &str| BackupScheduleSettings {
            database: "sales".into(),
            target: "s3://b/nightly".into(),
            cron: cron.into(),
            keep: 2,
        };
        let jobs = BackupJobs::new(&[schedule("*/5 * * * *"), schedule("bad")]);
        assert_eq!(jobs.jobs.len(), 1);
        assert_eq!(jobs.in_flight(), 0);
    }

    #[test]
    fn each_outcome_is_recorded_in_history_and_metrics() {
        let metrics = SystemMetrics::new();
        let ok = Ok(BackupRun {
            envelope_uri: "s3://b/nightly/sales-1.ndbb".into(),
            bytes: 10,
            deleted: 2,
        });
        let run = record_outcome("backup:sales", 7, 1_000, 3_500, &ok, Some(&metrics));
        assert!(run.success);
        assert_eq!((run.database_id, run.duration_ms), (7, 2_500));
        assert_eq!(
            metrics.backup_schedule_runs_total.load(Ordering::Relaxed),
            1
        );
        assert_eq!(
            metrics
                .backup_schedule_envelopes_deleted_total
                .load(Ordering::Relaxed),
            2
        );
        assert_eq!(
            metrics
                .backup_schedule_last_success_timestamp_seconds
                .load(Ordering::Relaxed),
            3
        );

        let failed = Err(crate::Error::BadRequest {
            detail: "no such database".into(),
        });
        let run = record_outcome("backup:sales", 7, 4_000, 4_000, &failed, Some(&metrics));
        assert!(!run.success);
        assert!(run.error.unwrap().contains("no such database"));
        assert_eq!(
            metrics
                .backup_schedule_failures_total
                .load(Ordering::Relaxed),
            1
        );
    }

    #[test]
    fn history_keeps_backup_runs_by_job_name() {
        let dir = tempfile::tempdir().unwrap();
        let history = JobHistoryStore::open(dir.path()).unwrap();
        let ok = Ok(BackupRun {
            envelope_uri: "u".into(),
            bytes: 1,
            deleted: 0,
        });
        history
            .record(record_outcome("backup:sales:s3://b/p", 7, 1, 2, &ok, None))
            .unwrap();
        assert!(
            history
                .last_run(7, 0, "backup:sales:s3://b/p")
                .unwrap()
                .success
        );
    }
}
