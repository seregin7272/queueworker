package integration

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/seregin7272/queueworker"
	"github.com/seregin7272/queueworker/pgstore"
)

var pool *pgxpool.Pool

func TestMain(m *testing.M) {
	ctx := context.Background()

	container, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("queueworker"),
		tcpostgres.WithUsername("test"),
		tcpostgres.WithPassword("test"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(time.Minute),
		),
	)
	if err != nil {
		panic(err)
	}

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		panic(err)
	}

	pool, err = pgxpool.New(ctx, dsn)
	if err != nil {
		panic(err)
	}

	if _, err := pool.Exec(ctx, pgstore.Schema); err != nil {
		panic(err)
	}

	code := m.Run()

	pool.Close()
	_ = container.Terminate(ctx)
	os.Exit(code)
}

type payload struct {
	Name string `json:"name"`
}

type jobRow struct {
	State       string
	Attempts    int
	ScheduledAt time.Time
	LockedBy    *uuid.UUID
	LeaseUntil  *time.Time
	LastError   *string
}

func reset(t *testing.T) {
	t.Helper()

	if _, err := pool.Exec(context.Background(), `TRUNCATE queueworker_jobs, queueworker_failed_jobs`); err != nil {
		t.Fatalf("truncate: %v", err)
	}
}

func getJob(t *testing.T, id int64) (jobRow, bool) {
	t.Helper()

	var j jobRow
	err := pool.QueryRow(context.Background(),
		`SELECT state, attempts, scheduled_at, locked_by, lease_until, last_error FROM queueworker_jobs WHERE id = $1`, id,
	).Scan(&j.State, &j.Attempts, &j.ScheduledAt, &j.LockedBy, &j.LeaseUntil, &j.LastError)
	if errors.Is(err, pgx.ErrNoRows) {
		return jobRow{}, false
	}
	if err != nil {
		t.Fatalf("get job: %v", err)
	}

	return j, true
}

func newStore(t *testing.T, queue string) *pgstore.Store[payload] {
	t.Helper()

	s, err := pgstore.New[payload](pool, queue)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}

	return s
}

func insert(t *testing.T, queue, name string, opts pgstore.InsertOptions) int64 {
	t.Helper()

	id, err := pgstore.Insert(context.Background(), pool, queue, payload{Name: name}, opts)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	return id
}

func acquireOne(t *testing.T, s *pgstore.Store[payload]) queueworker.Job[payload] {
	t.Helper()

	jobs, err := s.Acquire(context.Background(), 10, time.Minute, uuid.New())
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("acquired %d jobs, want 1", len(jobs))
	}

	return jobs[0]
}

func TestAcquireRespectsQueueScheduleAndLocks(t *testing.T) {
	reset(t)
	ctx := context.Background()

	now := insert(t, "q", "now", pgstore.InsertOptions{})
	insert(t, "q", "later", pgstore.InsertOptions{ScheduledAt: time.Now().Add(time.Hour)})
	insert(t, "other", "other", pgstore.InsertOptions{})

	s := newStore(t, "q")
	token := uuid.New()

	jobs, err := s.Acquire(ctx, 10, time.Minute, token)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if len(jobs) != 1 || jobs[0].ID != now || jobs[0].Data.Name != "now" || jobs[0].Token != token {
		t.Fatalf("jobs = %+v, want only the due job of queue q", jobs)
	}

	again, err := s.Acquire(ctx, 10, time.Minute, uuid.New())
	if err != nil {
		t.Fatalf("second acquire: %v", err)
	}
	if len(again) != 0 {
		t.Fatalf("running job acquired twice: %+v", again)
	}

	j, _ := getJob(t, now)
	if j.State != "running" || j.LockedBy == nil || *j.LockedBy != token || j.LeaseUntil == nil {
		t.Fatalf("job after acquire: %+v", j)
	}
}

// Записи с чужим токеном ничего не меняют: опоздавшая попытка не перезапишет новую.
func TestSettleIsFencedByToken(t *testing.T) {
	reset(t)
	ctx := context.Background()

	id := insert(t, "q", "a", pgstore.InsertOptions{})
	s := newStore(t, "q")
	job := acquireOne(t, s)

	stale := job
	stale.Token = uuid.New()

	checks := map[string]func() (bool, error){
		"complete": func() (bool, error) { return s.Complete(ctx, stale) },
		"retry": func() (bool, error) {
			return s.Retry(ctx, stale, queueworker.RetryDecision{Attempts: 1, At: time.Now()}, errors.New("x"))
		},
		"fail":    func() (bool, error) { return s.Fail(ctx, stale, errors.New("x")) },
		"snooze":  func() (bool, error) { return s.Snooze(ctx, stale, time.Now(), errors.New("x")) },
		"release": func() (bool, error) { return s.Release(ctx, stale) },
	}
	for name, op := range checks {
		ok, err := op()
		if err != nil || ok {
			t.Fatalf("%s with stale token: ok=%v err=%v, want false", name, ok, err)
		}
	}

	if j, exists := getJob(t, id); !exists || j.State != "running" {
		t.Fatalf("job changed by stale token: %+v exists=%v", j, exists)
	}

	if ok, err := s.Complete(ctx, job); err != nil || !ok {
		t.Fatalf("complete with own token: ok=%v err=%v", ok, err)
	}
	if _, exists := getJob(t, id); exists {
		t.Fatal("completed job must be deleted")
	}
}

func TestRetrySnoozeRelease(t *testing.T) {
	reset(t)
	ctx := context.Background()
	s := newStore(t, "q")

	id := insert(t, "q", "a", pgstore.InsertOptions{})
	at := time.Now().Add(time.Hour).Truncate(time.Microsecond)

	job := acquireOne(t, s)
	if ok, err := s.Retry(ctx, job, queueworker.RetryDecision{Attempts: 1, At: at}, errors.New("boom")); err != nil || !ok {
		t.Fatalf("retry: ok=%v err=%v", ok, err)
	}
	j, _ := getJob(t, id)
	if j.State != "available" || j.Attempts != 1 || !j.ScheduledAt.Equal(at) || j.LockedBy != nil || j.LastError == nil || *j.LastError != "boom" {
		t.Fatalf("after retry: %+v", j)
	}

	if _, err := pool.Exec(ctx, `UPDATE queueworker_jobs SET scheduled_at = now() WHERE id = $1`, id); err != nil {
		t.Fatal(err)
	}
	job = acquireOne(t, s)
	if job.Attempts != 1 {
		t.Fatalf("attempts after reacquire = %d, want 1", job.Attempts)
	}
	if ok, err := s.Snooze(ctx, job, at, errors.New("breaker open")); err != nil || !ok {
		t.Fatalf("snooze: ok=%v err=%v", ok, err)
	}
	if j, _ := getJob(t, id); j.Attempts != 1 || !j.ScheduledAt.Equal(at) {
		t.Fatalf("snooze must keep attempts: %+v", j)
	}

	if _, err := pool.Exec(ctx, `UPDATE queueworker_jobs SET scheduled_at = now() WHERE id = $1`, id); err != nil {
		t.Fatal(err)
	}
	job = acquireOne(t, s)
	if ok, err := s.Release(ctx, job); err != nil || !ok {
		t.Fatalf("release: ok=%v err=%v", ok, err)
	}
	if j, _ := getJob(t, id); j.State != "available" || j.Attempts != 1 || j.LockedBy != nil {
		t.Fatalf("after release: %+v", j)
	}
}

func TestFailMovesToFailedTable(t *testing.T) {
	reset(t)
	ctx := context.Background()
	s := newStore(t, "q")

	id := insert(t, "q", "a", pgstore.InsertOptions{})
	job := acquireOne(t, s)

	if ok, err := s.Fail(ctx, job, errors.New("bad payload")); err != nil || !ok {
		t.Fatalf("fail: ok=%v err=%v", ok, err)
	}
	if _, exists := getJob(t, id); exists {
		t.Fatal("failed job must leave the queue")
	}

	var attempts int
	var lastErr, name string
	err := pool.QueryRow(ctx,
		`SELECT attempts, last_error, payload->>'name' FROM queueworker_failed_jobs WHERE id = $1`, id,
	).Scan(&attempts, &lastErr, &name)
	if err != nil {
		t.Fatalf("failed job row: %v", err)
	}
	if attempts != 1 || lastErr != "bad payload" || name != "a" {
		t.Fatalf("failed row: attempts=%d err=%q name=%q", attempts, lastErr, name)
	}
}

func TestRescueReturnsExpiredLeasesOnly(t *testing.T) {
	reset(t)
	ctx := context.Background()
	s := newStore(t, "q")

	expired := insert(t, "q", "expired", pgstore.InsertOptions{})
	alive := insert(t, "q", "alive", pgstore.InsertOptions{})
	if _, err := s.Acquire(ctx, 10, time.Minute, uuid.New()); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE queueworker_jobs SET lease_until = now() - interval '1 second' WHERE id = $1`, expired); err != nil {
		t.Fatal(err)
	}

	n, err := s.Rescue(ctx)
	if err != nil || n != 1 {
		t.Fatalf("rescue: n=%d err=%v, want 1", n, err)
	}
	if j, _ := getJob(t, expired); j.State != "available" || j.LockedBy != nil {
		t.Fatalf("expired job after rescue: %+v", j)
	}
	if j, _ := getJob(t, alive); j.State != "running" {
		t.Fatalf("job with live lease must stay running: %+v", j)
	}
}

// Задача, поставленная в откатившейся транзакции, не появляется.
func TestInsertInTransaction(t *testing.T) {
	reset(t)
	ctx := context.Background()

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	id, err := pgstore.Insert(ctx, tx, "q", payload{Name: "tx"}, pgstore.InsertOptions{})
	if err != nil {
		t.Fatalf("insert in tx: %v", err)
	}
	_ = tx.Rollback(ctx)

	if _, exists := getJob(t, id); exists {
		t.Fatal("job from rolled back transaction must not exist")
	}
}

func TestUndecodablePayloadIsFailed(t *testing.T) {
	reset(t)
	ctx := context.Background()

	var id int64
	if err := pool.QueryRow(ctx,
		`INSERT INTO queueworker_jobs (queue, payload, max_attempts) VALUES ('q', '"not an object"', 3) RETURNING id`,
	).Scan(&id); err != nil {
		t.Fatal(err)
	}

	jobs, err := newStore(t, "q").Acquire(ctx, 10, time.Minute, uuid.New())
	if err != nil || len(jobs) != 0 {
		t.Fatalf("acquire: jobs=%v err=%v, want none", jobs, err)
	}

	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM queueworker_failed_jobs WHERE id = $1`, id).Scan(&n); err != nil || n != 1 {
		t.Fatalf("undecodable job must be failed: n=%d err=%v", n, err)
	}
}

// Полный цикл: воркер на pgstore выполняет задачи, повторяет упавшую и не теряет ни одной.
func TestWorkerEndToEnd(t *testing.T) {
	reset(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const total = 20
	for i := range total {
		name := "ok"
		if i == 0 {
			name = "flaky"
		}
		insert(t, "e2e", name, pgstore.InsertOptions{MaxAttempts: 5})
	}

	var flakyCalls int
	handler := queueworker.HandlerFunc[payload](func(_ context.Context, job queueworker.Job[payload]) error {
		if job.Data.Name == "flaky" && job.Attempts == 0 {
			flakyCalls++
			return errors.New("first attempt fails")
		}
		return nil
	})

	w, err := queueworker.New[payload](queueworker.Config{
		Workers:        2,
		Concurrency:    4,
		PollInterval:   10 * time.Millisecond,
		LeaseDuration:  time.Minute,
		JobTimeout:     10 * time.Second,
		RescueInterval: time.Second,
	}, newStore(t, "e2e"), handler,
		queueworker.WithRetryPolicy[payload](queueworker.RetryPolicyFunc(
			func(attempts, _ int, now time.Time, _ error) queueworker.RetryDecision {
				return queueworker.RetryDecision{Attempts: attempts + 1, At: now}
			},
		)),
	)
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan struct{})
	go func() {
		w.Run(ctx)
		close(done)
	}()

	deadline := time.Now().Add(10 * time.Second)
	for {
		var n int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM queueworker_jobs WHERE queue = 'e2e'`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d jobs left in queue", n)
		}
		time.Sleep(20 * time.Millisecond)
	}

	cancel()
	<-done

	if flakyCalls != 1 {
		t.Fatalf("flaky job failed %d times, want 1", flakyCalls)
	}
}
