-- Reverse 0014. refused_hour is token-push bookkeeping; without it the previous
-- release re-sends parked hours on every run, which is safe (the manager refuses
-- them again and stores nothing).
DROP TABLE IF EXISTS refused_hour;
