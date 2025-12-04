-- Номер миграции: 002
-- Описание: добавляем поле language в таблицу users

BEGIN;

ALTER TABLE users
    ADD COLUMN IF NOT EXISTS language TEXT NOT NULL DEFAULT 'ru';

COMMIT;
