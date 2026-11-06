-- Схема очереди queueworker. Применяется миграцией приложения.

CREATE TABLE IF NOT EXISTS queueworker_jobs (
    id           BIGSERIAL   PRIMARY KEY,
    queue        TEXT        NOT NULL,
    payload      JSONB       NOT NULL,
    state        TEXT        NOT NULL DEFAULT 'available' CHECK (state IN ('available', 'running')),
    attempts     INT         NOT NULL DEFAULT 0,
    max_attempts INT         NOT NULL,
    scheduled_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    attempted_at TIMESTAMPTZ,
    lease_until  TIMESTAMPTZ,
    locked_by    UUID,
    last_error   TEXT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Захват: доступные задачи очереди по времени запуска.
CREATE INDEX IF NOT EXISTS queueworker_jobs_available_idx
    ON queueworker_jobs (queue, scheduled_at, id) WHERE state = 'available';

-- Rescuer: выполняющиеся задачи по сроку аренды.
CREATE INDEX IF NOT EXISTS queueworker_jobs_running_lease_idx
    ON queueworker_jobs (lease_until) WHERE state = 'running';

-- Задачи, завершившиеся окончательной ошибкой. Выполненные задачи удаляются.
CREATE TABLE IF NOT EXISTS queueworker_failed_jobs (
    id         BIGINT      PRIMARY KEY,
    queue      TEXT        NOT NULL,
    payload    JSONB       NOT NULL,
    attempts   INT         NOT NULL,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL,
    failed_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
