-- Reverse 0012. parked_usage is the only durable record of conflicted usage that
-- has not been billed, so this REFUSES while any unresolved row exists.
-- pushed_usage and pushed_hour are bookkeeping and are dropped.
DO $$
DECLARE
    parked bigint;
BEGIN
    SELECT COUNT(*) INTO parked FROM parked_usage WHERE resolved_at IS NULL;
    IF parked > 0 THEN
        RAISE EXCEPTION '0012 down: % parked_usage rows are unresolved (unbilled usage). Resolve them or export them before migrating down.', parked;
    END IF;
END $$;

DROP TABLE IF EXISTS pushed_hour;
DROP TABLE IF EXISTS pushed_usage;
DROP TABLE IF EXISTS parked_usage;
