// Package pgstore — хранилище queueworker на PostgreSQL (pgx).
//
// Таблицы описаны в Schema; их нужно создать миграцией приложения. Задачи кладутся
// через Insert — в том числе внутри транзакции приложения, вместе с бизнес-данными.
package pgstore

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/seregin7272/queueworker"
)

// Schema — DDL таблиц очереди.
//
//go:embed schema.sql
var Schema string

const maxErrorLen = 1000

// Querier — то, через что Insert кладёт задачу: *pgxpool.Pool, *pgx.Conn или pgx.Tx.
type Querier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// InsertOptions — параметры новой задачи.
type InsertOptions struct {
	// MaxAttempts — лимит попыток; по умолчанию 25.
	MaxAttempts int
	// ScheduledAt — когда задачу можно брать; по умолчанию сразу.
	ScheduledAt time.Time
}

const defaultMaxAttempts = 25

// Insert кладёт задачу в очередь queue. Передайте pgx.Tx, чтобы задача появилась
// атомарно вместе с данными приложения.
func Insert[T any](ctx context.Context, q Querier, queue string, payload T, opts InsertOptions) (int64, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return 0, fmt.Errorf("pgstore: marshal payload: %w", err)
	}

	maxAttempts := opts.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = defaultMaxAttempts
	}

	var scheduledAt *time.Time
	if !opts.ScheduledAt.IsZero() {
		scheduledAt = &opts.ScheduledAt
	}

	var id int64

	err = q.QueryRow(ctx,
		`INSERT INTO queueworker_jobs (queue, payload, max_attempts, scheduled_at)
		 VALUES ($1, $2, $3, COALESCE($4, now()))
		 RETURNING id`,
		queue, data, maxAttempts, scheduledAt,
	).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("pgstore: insert job: %w", err)
	}

	return id, nil
}

// Store — очередь queue в таблице queueworker_jobs. Payload хранится как JSON и
// разбирается в T.
type Store[T any] struct {
	pool  *pgxpool.Pool
	queue string
}

var _ queueworker.Store[struct{}] = (*Store[struct{}])(nil)

// New создаёт хранилище очереди queue.
func New[T any](pool *pgxpool.Pool, queue string) (*Store[T], error) {
	if pool == nil {
		return nil, errors.New("pgstore: pool is required")
	}
	if queue == "" {
		return nil, errors.New("pgstore: queue is required")
	}

	return &Store[T]{pool: pool, queue: queue}, nil
}

func (s *Store[T]) Acquire(ctx context.Context, limit int, lease time.Duration, token uuid.UUID) ([]queueworker.Job[T], error) {
	rows, err := s.pool.Query(ctx,
		`WITH picked AS (
		     SELECT id FROM queueworker_jobs
		     WHERE queue = $1 AND state = 'available' AND scheduled_at <= now()
		     ORDER BY scheduled_at, id
		     FOR UPDATE SKIP LOCKED
		     LIMIT $2
		 )
		 UPDATE queueworker_jobs j
		 SET state = 'running', attempted_at = now(), lease_until = now() + $3::interval,
		     locked_by = $4, updated_at = now()
		 FROM picked
		 WHERE j.id = picked.id
		 RETURNING j.id, j.attempts, j.max_attempts, j.payload`,
		s.queue, limit, lease.String(), token,
	)
	if err != nil {
		return nil, fmt.Errorf("pgstore: acquire: %w", err)
	}

	type row struct {
		ID          int64
		Attempts    int
		MaxAttempts int
		Payload     []byte
	}

	raw, err := pgx.CollectRows(rows, pgx.RowToStructByPos[row])
	if err != nil {
		return nil, fmt.Errorf("pgstore: acquire: %w", err)
	}

	jobs := make([]queueworker.Job[T], 0, len(raw))
	for _, r := range raw {
		var data T
		if err := json.Unmarshal(r.Payload, &data); err != nil {
			// Задача уже захвачена: не бросаем её до истечения аренды, а сразу уводим
			// в ошибки — разобрать её не получится и при повторе.
			job := queueworker.Job[T]{ID: r.ID, Attempts: r.Attempts, MaxAttempts: r.MaxAttempts, Token: token}
			if _, failErr := s.Fail(ctx, job, fmt.Errorf("unmarshal payload: %w", err)); failErr != nil {
				return nil, fmt.Errorf("pgstore: fail undecodable job %d: %w", r.ID, failErr)
			}

			continue
		}

		jobs = append(jobs, queueworker.Job[T]{
			ID:          r.ID,
			Attempts:    r.Attempts,
			MaxAttempts: r.MaxAttempts,
			Token:       token,
			Data:        data,
		})
	}

	return jobs, nil
}

func (s *Store[T]) Complete(ctx context.Context, job queueworker.Job[T]) (bool, error) {
	tag, err := s.pool.Exec(ctx,
		`DELETE FROM queueworker_jobs WHERE id = $1 AND state = 'running' AND locked_by = $2`,
		job.ID, job.Token,
	)

	return affected(tag, err, "complete")
}

func (s *Store[T]) Retry(ctx context.Context, job queueworker.Job[T], decision queueworker.RetryDecision, cause error) (bool, error) {
	tag, err := s.pool.Exec(ctx,
		`UPDATE queueworker_jobs
		 SET state = 'available', attempts = $3, scheduled_at = $4, last_error = $5,
		     lease_until = NULL, locked_by = NULL, updated_at = now()
		 WHERE id = $1 AND state = 'running' AND locked_by = $2`,
		job.ID, job.Token, decision.Attempts, decision.At, errorText(cause),
	)

	return affected(tag, err, "retry")
}

// Fail переносит задачу в queueworker_failed_jobs в одной транзакции с удалением.
func (s *Store[T]) Fail(ctx context.Context, job queueworker.Job[T], cause error) (bool, error) {
	var moved bool

	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx,
			`WITH deleted AS (
			     DELETE FROM queueworker_jobs
			     WHERE id = $1 AND state = 'running' AND locked_by = $2
			     RETURNING id, queue, payload, created_at
			 )
			 INSERT INTO queueworker_failed_jobs (id, queue, payload, attempts, last_error, created_at)
			 SELECT id, queue, payload, $3, $4, created_at FROM deleted`,
			job.ID, job.Token, job.Attempts+1, errorText(cause),
		)
		if err != nil {
			return err
		}

		moved = tag.RowsAffected() == 1

		return nil
	})
	if err != nil {
		return false, fmt.Errorf("pgstore: fail: %w", err)
	}

	return moved, nil
}

func (s *Store[T]) Snooze(ctx context.Context, job queueworker.Job[T], at time.Time, cause error) (bool, error) {
	tag, err := s.pool.Exec(ctx,
		`UPDATE queueworker_jobs
		 SET state = 'available', scheduled_at = $3, last_error = $4,
		     lease_until = NULL, locked_by = NULL, updated_at = now()
		 WHERE id = $1 AND state = 'running' AND locked_by = $2`,
		job.ID, job.Token, at, errorText(cause),
	)

	return affected(tag, err, "snooze")
}

func (s *Store[T]) Release(ctx context.Context, job queueworker.Job[T]) (bool, error) {
	tag, err := s.pool.Exec(ctx,
		`UPDATE queueworker_jobs
		 SET state = 'available', scheduled_at = now(),
		     lease_until = NULL, locked_by = NULL, updated_at = now()
		 WHERE id = $1 AND state = 'running' AND locked_by = $2`,
		job.ID, job.Token,
	)

	return affected(tag, err, "release")
}

func (s *Store[T]) Rescue(ctx context.Context) (int64, error) {
	tag, err := s.pool.Exec(ctx,
		`UPDATE queueworker_jobs
		 SET state = 'available', lease_until = NULL, locked_by = NULL, updated_at = now()
		 WHERE queue = $1 AND state = 'running' AND lease_until < now()`,
		s.queue,
	)
	if err != nil {
		return 0, fmt.Errorf("pgstore: rescue: %w", err)
	}

	return tag.RowsAffected(), nil
}

func affected(tag pgconn.CommandTag, err error, op string) (bool, error) {
	if err != nil {
		return false, fmt.Errorf("pgstore: %s: %w", op, err)
	}

	return tag.RowsAffected() == 1, nil
}

func errorText(err error) *string {
	if err == nil {
		return nil
	}

	runes := []rune(err.Error())
	if len(runes) > maxErrorLen {
		runes = runes[:maxErrorLen]
	}

	text := string(runes)

	return &text
}
