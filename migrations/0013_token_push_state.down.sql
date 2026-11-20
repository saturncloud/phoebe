-- Reverse 0013. token_push_state is token-push bookkeeping; the floor it holds
-- is recomputed as min(pushed_hour.window_start) by the previous release.
DROP TABLE IF EXISTS token_push_state;
