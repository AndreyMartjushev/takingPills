CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    telegram_id BIGINT UNIQUE NOT NULL,
    first_name TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    timezone TEXT NOT NULL DEFAULT 'UTC',
    language TEXT NOT NULL DEFAULT 'ru',
    remind_before_minutes INT NOT NULL DEFAULT 10
);

CREATE TABLE IF NOT EXISTS medications (
    id SERIAL PRIMARY KEY,
    user_id INT REFERENCES users(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    times TEXT[] NOT NULL,           -- массив строк типа '08:00'
    doses_per_day INT NOT NULL DEFAULT 1,
    schedule_mode TEXT NOT NULL DEFAULT 'exact',
    periods TEXT[],
    is_active BOOLEAN DEFAULT TRUE,
    paused_until TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS intakes (
    id SERIAL PRIMARY KEY,
    medication_id INT REFERENCES medications(id) ON DELETE CASCADE,
    scheduled_at TIMESTAMPTZ NOT NULL,   -- конкретное время для приёма
    taken BOOLEAN DEFAULT FALSE,
    reminder_sent BOOLEAN DEFAULT FALSE,
    taken_at TIMESTAMPTZ,
    next_reminder_at TIMESTAMPTZ,
    last_reminder_at TIMESTAMPTZ,
    reminders_paused BOOLEAN DEFAULT FALSE,
    CONSTRAINT intakes_medication_schedule_key UNIQUE (medication_id, scheduled_at)
);

CREATE INDEX IF NOT EXISTS idx_medications_user_id
    ON medications (user_id);

CREATE INDEX IF NOT EXISTS idx_intakes_medication_id
    ON intakes (medication_id);
