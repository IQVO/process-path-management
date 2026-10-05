-- Optimistic concurrency (ADR 0017): a version column on both mutable
-- aggregates, following workforce-management ADR-0021's pattern. Both
-- process_paths and cpt_schedules are written read-modify-write
-- (FindByID -> mutate -> Save), so a blind upsert lets two concurrent
-- callers silently clobber each other. Existing rows all start at 1,
-- matching what a fresh Define constructs in Go.
ALTER TABLE process_paths ADD COLUMN version INTEGER NOT NULL DEFAULT 1;
ALTER TABLE cpt_schedules ADD COLUMN version INTEGER NOT NULL DEFAULT 1;
