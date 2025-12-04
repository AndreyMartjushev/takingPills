-- 004_add_paused_until.sql
-- Добавляет столбец paused_until в medications для хранения даты автоматического возобновления курса.

BEGIN;

ALTER TABLE medications
    ADD COLUMN IF NOT EXISTS paused_until TIMESTAMPTZ;

COMMIT;
