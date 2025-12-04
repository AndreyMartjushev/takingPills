-- Номер миграции: 003
-- Описание: добавляем поле remind_before_minutes

BEGIN;

ALTER TABLE users
    ADD COLUMN IF NOT EXISTS remind_before_minutes INT NOT NULL DEFAULT 10;

COMMIT;
