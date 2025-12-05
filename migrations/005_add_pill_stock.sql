-- 005_add_pill_stock.sql
-- Добавляет учёт дозировки и остатков для лекарств.

BEGIN;

ALTER TABLE medications
    ADD COLUMN IF NOT EXISTS pills_per_dose INT NOT NULL DEFAULT 1,
    ADD COLUMN IF NOT EXISTS stock_total INT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS low_stock_notified BOOLEAN NOT NULL DEFAULT FALSE;

COMMIT;
