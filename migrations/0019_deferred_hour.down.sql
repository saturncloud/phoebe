-- Reverse 0019. deferred_hour is token-push bookkeeping. Without it the
-- previous release re-sends a deferred hour that was never accepted (it is an
-- unsent hour again), and stops its run at the first such refusal, as before.
DROP TABLE IF EXISTS deferred_hour;
