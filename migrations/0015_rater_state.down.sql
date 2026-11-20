-- Reverse 0015. rater_state is rater bookkeeping; the previous release has no
-- catch-up and rates only its trailing window. The rated_usage rows the
-- catch-up wrote stay: they are ordinary rollups.
DROP TABLE IF EXISTS rater_state;
