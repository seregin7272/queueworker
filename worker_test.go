package queueworker

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

// memStore — хранилище в памяти, соблюдающее контракт Store: записи только по токену.
type memStore struct {
	mu        sync.Mutex
	jobs      map[int64]*memJob
	acquires  []int // limit каждого захвата
	tokens    []uuid.UUID
	completed []int64
	failed    []int64
	settleErr []error // ctx.Err() в момент записи результата
	rescued   int
	maxID     int64
}

type memJob struct {
	job      Job[string]
	running  bool
	token    uuid.UUID
	at       time.Time
	attempts int
	lastErr  error
}

func newMemStore(payloads ...string) *memStore {
	s := &memStore{jobs: map[int64]*memJob{}}
	for i, p := range payloads {
		id := int64(i + 1)
		s.jobs[id] = &memJob{job: Job[string]{ID: id, MaxAttempts: 3, Data: p}}
		s.maxID = id
	}

	return s
}

func (s *memStore) Acquire(_ context.Context, limit int, _ time.Duration, token uuid.UUID) ([]Job[string], error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.acquires = append(s.acquires, limit)
	s.tokens = append(s.tokens, token)

	var res []Job[string]
	for id := int64(1); id <= s.maxID && len(res) < limit; id++ {
		j, ok := s.jobs[id]
		if !ok || j.running || time.Now().Before(j.at) {
			continue
		}

		j.running, j.token = true, token
		job := j.job
		job.Attempts, job.Token = j.attempts, token
		res = append(res, job)
	}

	return res, nil
}

func (s *memStore) owned(ctx context.Context, job Job[string]) (*memJob, bool) {
	s.settleErr = append(s.settleErr, ctx.Err())

	j, ok := s.jobs[job.ID]
	if !ok || !j.running || j.token != job.Token {
		return nil, false
	}

	return j, true
}

func (s *memStore) Complete(ctx context.Context, job Job[string]) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.owned(ctx, job); !ok {
		return false, nil
	}

	delete(s.jobs, job.ID)
	s.completed = append(s.completed, job.ID)

	return true, nil
}

func (s *memStore) Retry(ctx context.Context, job Job[string], d RetryDecision, cause error) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	j, ok := s.owned(ctx, job)
	if !ok {
		return false, nil
	}

	j.running, j.attempts, j.at, j.lastErr = false, d.Attempts, d.At, cause

	return true, nil
}

func (s *memStore) Fail(ctx context.Context, job Job[string], cause error) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	j, ok := s.owned(ctx, job)
	if !ok {
		return false, nil
	}

	j.lastErr = cause
	delete(s.jobs, job.ID)
	s.failed = append(s.failed, job.ID)

	return true, nil
}

func (s *memStore) Snooze(ctx context.Context, job Job[string], at time.Time, cause error) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	j, ok := s.owned(ctx, job)
	if !ok {
		return false, nil
	}

	j.running, j.at, j.lastErr = false, at, cause

	return true, nil
}

func (s *memStore) Release(ctx context.Context, job Job[string]) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	j, ok := s.owned(ctx, job)
	if !ok {
		return false, nil
	}

	j.running, j.at = false, time.Time{}

	return true, nil
}

func (s *memStore) Rescue(context.Context) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.rescued++

	return 0, nil
}

func (s *memStore) get(id int64) (memJob, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	j, ok := s.jobs[id]
	if !ok {
		return memJob{}, false
	}

	return *j, true
}

// recObserver запоминает итоги задач.
type recObserver struct {
	NopObserver[string]
	mu      sync.Mutex
	results map[int64]Result
}

func newRecObserver() *recObserver { return &recObserver{results: map[int64]Result{}} }

func (o *recObserver) JobFinished(_ context.Context, job Job[string], r Result) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.results[job.ID] = r
}

func (o *recObserver) result(id int64) (Result, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	r, ok := o.results[id]

	return r, ok
}

func testConfig() Config {
	return Config{
		Concurrency:    4,
		PollInterval:   5 * time.Millisecond,
		LeaseDuration:  time.Minute,
		JobTimeout:     time.Second,
		RescueInterval: time.Hour,
	}
}

func newTestWorker(t *testing.T, cfg Config, store *memStore, h HandlerFunc[string], opts ...Option[string]) (*Worker[string], *recObserver) {
	t.Helper()

	obs := newRecObserver()
	opts = append([]Option[string]{
		WithObserver[string](obs),
		WithRetryPolicy[string](QuadraticBackoff{Base: time.Hour}),
	}, opts...)

	w, err := New[string](cfg, store, h, opts...)
	if err != nil {
		t.Fatalf("new worker: %v", err)
	}

	return w, obs
}

// runUntil запускает воркер и останавливает его, когда у всех ids появился итог.
func runUntil(t *testing.T, w *Worker[string], obs *recObserver, ids ...int64) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		w.Run(ctx)
		close(done)
	}()

	waitFor(t, func() bool {
		for _, id := range ids {
			if _, ok := obs.result(id); !ok {
				return false
			}
		}
		return true
	})

	cancel()
	<-done
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()

	deadline := time.Now().Add(3 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met in time")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestNewValidatesConfig(t *testing.T) {
	h := HandlerFunc[string](func(context.Context, Job[string]) error { return nil })

	if _, err := New[string](Config{LeaseDuration: time.Minute, JobTimeout: time.Minute}, newMemStore(), h); err == nil {
		t.Fatal("job timeout equal to lease must be rejected")
	}
	if _, err := New[string](Config{Concurrency: -1}, newMemStore(), h); err == nil {
		t.Fatal("negative concurrency must be rejected")
	}

	w, err := New[string](Config{LeaseDuration: 30 * time.Second}, newMemStore(), h)
	if err != nil {
		t.Fatalf("short lease with default job timeout: %v", err)
	}
	if w.cfg.JobTimeout != 20*time.Second {
		t.Fatalf("default job timeout = %s, want 2/3 of lease", w.cfg.JobTimeout)
	}
}

func TestOutcomes(t *testing.T) {
	errBoom := errors.New("boom")

	cases := []struct {
		name    string
		handle  func(ctx context.Context) error
		outcome Outcome
		check   func(t *testing.T, s *memStore, r Result)
	}{
		{
			name:    "success completes",
			handle:  func(context.Context) error { return nil },
			outcome: OutcomeCompleted,
			check: func(t *testing.T, s *memStore, _ Result) {
				if len(s.completed) != 1 {
					t.Fatalf("completed = %v", s.completed)
				}
			},
		},
		{
			name:    "error retries with policy",
			handle:  func(context.Context) error { return errBoom },
			outcome: OutcomeRetried,
			check: func(t *testing.T, s *memStore, r Result) {
				j, _ := s.get(1)
				if j.attempts != 1 || !errors.Is(j.lastErr, errBoom) || r.Retry.Attempts != 1 {
					t.Fatalf("attempts=%d lastErr=%v decision=%+v", j.attempts, j.lastErr, r.Retry)
				}
			},
		},
		{
			name:    "permanent fails",
			handle:  func(context.Context) error { return Permanent(errBoom) },
			outcome: OutcomeFailed,
			check: func(t *testing.T, s *memStore, r Result) {
				if len(s.failed) != 1 || !errors.Is(r.Err, errBoom) {
					t.Fatalf("failed = %v err = %v", s.failed, r.Err)
				}
			},
		},
		{
			name:    "snooze keeps attempts",
			handle:  func(context.Context) error { return Snooze(time.Hour, errBoom) },
			outcome: OutcomeSnoozed,
			check: func(t *testing.T, s *memStore, _ Result) {
				j, _ := s.get(1)
				if j.attempts != 0 || time.Until(j.at) < 59*time.Minute {
					t.Fatalf("attempts=%d at=%s", j.attempts, j.at)
				}
			},
		},
		{
			name:    "panic retries",
			handle:  func(context.Context) error { panic("bad") },
			outcome: OutcomeRetried,
			check: func(t *testing.T, _ *memStore, r Result) {
				if !errors.Is(r.Err, ErrPanic) {
					t.Fatalf("err = %v, want ErrPanic", r.Err)
				}
			},
		},
		{
			// Ошибка после таймаута — даже «окончательная» — повтор, а не провал:
			// упала из-за ctx, а не по вине задачи.
			name: "timeout retries even permanent error",
			handle: func(ctx context.Context) error {
				<-ctx.Done()
				return Permanent(ctx.Err())
			},
			outcome: OutcomeRetried,
			check: func(t *testing.T, s *memStore, r Result) {
				if !errors.Is(r.Err, ErrJobTimeout) || len(s.failed) != 0 {
					t.Fatalf("err = %v failed = %v", r.Err, s.failed)
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := testConfig()
			cfg.JobTimeout = 20 * time.Millisecond

			store := newMemStore("a")
			w, obs := newTestWorker(t, cfg, store, func(ctx context.Context, _ Job[string]) error {
				return tc.handle(ctx)
			})

			runUntil(t, w, obs, 1)

			r, _ := obs.result(1)
			if r.Outcome != tc.outcome {
				t.Fatalf("outcome = %s, want %s (err %v)", r.Outcome, tc.outcome, r.Err)
			}
			tc.check(t, store, r)
		})
	}
}

func TestRetryExhaustedFails(t *testing.T) {
	store := newMemStore("a")
	store.jobs[1].attempts = 2 // MaxAttempts = 3

	w, obs := newTestWorker(t, testConfig(), store, func(context.Context, Job[string]) error {
		return errors.New("boom")
	})
	runUntil(t, w, obs, 1)

	if r, _ := obs.result(1); r.Outcome != OutcomeFailed || !r.Retry.Fail {
		t.Fatalf("result = %+v, want failed by exhausted attempts", r)
	}
}

// Остановка прерывает задачу: она возвращается в очередь без расхода попытки, а запись
// идёт с неотменённым контекстом.
func TestShutdownReleasesJob(t *testing.T) {
	store := newMemStore("a")
	started := make(chan struct{})

	w, obs := newTestWorker(t, testConfig(), store, func(ctx context.Context, _ Job[string]) error {
		close(started)
		<-ctx.Done()
		return Permanent(ctx.Err())
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		w.Run(ctx)
		close(done)
	}()

	<-started
	cancel()
	<-done

	r, ok := obs.result(1)
	if !ok || r.Outcome != OutcomeReleased {
		t.Fatalf("result = %+v, want released", r)
	}

	j, ok := store.get(1)
	if !ok || j.running || j.attempts != 0 || len(store.failed) != 0 {
		t.Fatalf("job after shutdown: %+v failed=%v", j, store.failed)
	}
	if err := store.settleErr[len(store.settleErr)-1]; err != nil {
		t.Fatalf("release got cancelled context: %v", err)
	}
}

// Успешная задача, завершившаяся уже после сигнала остановки, всё равно записывается.
func TestShutdownStillCompletesFinishedJob(t *testing.T) {
	store := newMemStore("a")
	started, release := make(chan struct{}), make(chan struct{})

	w, obs := newTestWorker(t, testConfig(), store, func(context.Context, Job[string]) error {
		close(started)
		<-release
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		w.Run(ctx)
		close(done)
	}()

	<-started
	cancel()
	close(release)
	<-done

	if r, _ := obs.result(1); r.Outcome != OutcomeCompleted || r.LostLease {
		t.Fatalf("result = %+v, want completed", r)
	}
}

// Опоздавшая попытка не записывает результат: хранилище сообщает о потере владения.
func TestLostLeaseReported(t *testing.T) {
	store := newMemStore("a")

	w, obs := newTestWorker(t, testConfig(), store, func(_ context.Context, job Job[string]) error {
		store.mu.Lock()
		store.jobs[job.ID].token = uuid.New() // задачу перезахватили
		store.mu.Unlock()
		return nil
	})
	runUntil(t, w, obs, 1)

	if r, _ := obs.result(1); r.Outcome != OutcomeCompleted || !r.LostLease {
		t.Fatalf("result = %+v, want lost lease", r)
	}
	if len(store.completed) != 0 {
		t.Fatal("stale attempt must not complete the job")
	}
}

// Медленная задача занимает только свой слот: следующий захват не ждёт её.
func TestSlowJobDoesNotBlockFreeSlots(t *testing.T) {
	store := newMemStore("slow", "fast", "next")
	release := make(chan struct{})

	cfg := testConfig()
	cfg.Concurrency = 2

	w, obs := newTestWorker(t, cfg, store, func(_ context.Context, job Job[string]) error {
		if job.Data == "slow" {
			<-release
		}
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		w.Run(ctx)
		close(done)
	}()

	waitFor(t, func() bool { _, ok := obs.result(3); return ok })
	if _, ok := obs.result(1); ok {
		t.Fatal("slow job must still be running")
	}

	close(release)
	waitFor(t, func() bool { _, ok := obs.result(1); return ok })
	cancel()
	<-done

	store.mu.Lock()
	defer store.mu.Unlock()
	if store.acquires[0] != 2 {
		t.Fatalf("first acquire limit = %d, want 2", store.acquires[0])
	}
	for _, limit := range store.acquires {
		if limit == 0 {
			t.Fatal("acquire must not be called without free slots")
		}
	}
}

func TestJobCarriesWorkerNumber(t *testing.T) {
	store := newMemStore("a")

	var got int
	w, obs := newTestWorker(t, testConfig(), store, func(_ context.Context, job Job[string]) error {
		got = job.Worker
		return nil
	})
	runUntil(t, w, obs, 1)

	if got != 1 {
		t.Fatalf("job.Worker = %d, want 1", got)
	}
}

func TestNewTokenPerAcquire(t *testing.T) {
	store := newMemStore("a", "b")

	cfg := testConfig()
	cfg.Concurrency = 1

	w, obs := newTestWorker(t, cfg, store, func(context.Context, Job[string]) error { return nil })
	runUntil(t, w, obs, 1, 2)

	store.mu.Lock()
	defer store.mu.Unlock()

	seen := map[uuid.UUID]bool{}
	for _, tok := range store.tokens {
		if tok == uuid.Nil || seen[tok] {
			t.Fatalf("token reused or nil: %v", store.tokens)
		}
		seen[tok] = true
	}
}

func TestRescuerRuns(t *testing.T) {
	store := newMemStore()

	cfg := testConfig()
	cfg.RescueInterval = 5 * time.Millisecond

	w, _ := newTestWorker(t, cfg, store, func(context.Context, Job[string]) error { return nil })

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		w.Run(ctx)
		close(done)
	}()

	waitFor(t, func() bool {
		store.mu.Lock()
		defer store.mu.Unlock()
		return store.rescued >= 2
	})
	cancel()
	<-done
}

func TestQuadraticBackoff(t *testing.T) {
	now := time.Now()
	p := QuadraticBackoff{Base: time.Second}

	if d := p.Next(0, 5, now, nil); d.Attempts != 1 || d.At.Sub(now) != 4*time.Second || d.Fail {
		t.Fatalf("first retry = %+v", d)
	}
	if d := p.Next(4, 5, now, nil); !d.Fail {
		t.Fatalf("last attempt must fail: %+v", d)
	}
	if d := p.Next(100, 0, now, nil); d.Fail {
		t.Fatal("max attempts 0 means unlimited")
	}
}
