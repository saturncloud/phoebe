-- rater_state: the rater's own single-row state.
--
-- The rater rates the trailing rateTrailingHours (24) hours on each run. An
-- install that ran phoebe with the rater CronJob suspended kept writing
-- billing_event (the drainer runs regardless, and the pruner is suspended by
-- default), but those hours were never rated, so token-push never sent them.
-- The rater's one-time CATCH-UP rates them: every billing_event hour before the
-- trailing window that has no rated_usage and no parked_usage row, oldest
-- first, a bounded number per run.
--
-- catchup_cursor        the last hour the catch-up rated; the next run
--                       continues after it. NULL: the catch-up has not
--                       started.
-- catchup_completed_at  when the catch-up found nothing left to rate. Once
--                       set, the rater rates only its trailing window.
--
-- No row means the catch-up has not started; the rater inserts it. Nothing is
-- backfilled: on an install whose rater always ran, the first catch-up finds
-- no never-rated hours (or only hours whose events were all withheld, which
-- rate to nothing again) and completes on its first run.
CREATE TABLE IF NOT EXISTS rater_state (
    singleton            BOOLEAN PRIMARY KEY DEFAULT true CHECK (singleton),
    catchup_cursor       TIMESTAMPTZ,
    catchup_completed_at TIMESTAMPTZ
);
