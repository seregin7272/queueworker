package queueworker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
)

// Worker захватывает задачи из Store и выполняет их обработчиком.
type Worker[T any] struct {
	cfg     Config
	store   Store[T]
	handler Handler[T]
	retry   RetryPolicy
	obs     Observer[T]
	now     func() time.Time
}

// Option настраивает Worker.
type Option[T any] func(*Worker[T])

// WithRetryPolicy задаёт политику повторов; по умолчанию QuadraticBackoff{Base: 4s, Jitter: 5s}.
func WithRetryPolicy[T any](p RetryPolicy) Option[T] {
	return func(w *Worker[T]) { w.retry = p }
}

// WithObserver подключает наблюдателя: логи, метрики, трейсинг.
func WithObserver[T any](o Observer[T]) Option[T] {
	return func(w *Worker[T]) { w.obs = o }
}

// New создаёт воркер. Ошибка — при неверной конфигурации.
func New[T any](cfg Config, store Store[T], handler Handler[T], opts ...Option[T]) (*Worker[T], error) {
	if store == nil {
		return nil, errors.New("queueworker: store is required")
	}
	if handler == nil {
		return nil, errors.New("queueworker: handler is required")
	}

	cfg = cfg.withDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	w := &Worker[T]{
		cfg:     cfg,
		store:   store,
		handler: handler,
		retry:   QuadraticBackoff{Base: 4 * time.Second, Jitter: 5 * time.Second},
		obs:     NopObserver[T]{},
		now:     time.Now,
	}
	for _, opt := range opts {
		opt(w)
	}

	return w, nil
}

// Run выполняет задачи до отмены ctx. После отмены новые задачи не берутся, задачи в
// работе получают отмену и успевают записать результат или вернуться в очередь;
// Run возвращается, когда все они завершились.
func (w *Worker[T]) Run(ctx context.Context) {
	var wg sync.WaitGroup

	for i := range w.cfg.Workers {
		wg.Add(1)
		go func(num int) {
			defer wg.Done()
			w.runLoop(ctx, num)
		}(i + 1)
	}

	wg.Go(func() {
		w.runRescuer(ctx)
	})

	wg.Wait()
}

func (w *Worker[T]) runLoop(ctx context.Context, num int) {
	slotList := newSlots(w.cfg.Concurrency)
	defer slotList.wait()

	ticker := time.NewTicker(w.cfg.PollInterval)
	defer ticker.Stop()

	for {
		if err := w.acquire(ctx, num, slotList); err != nil && ctx.Err() == nil {
			w.obs.Error(ctx, OpAcquire, err)
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// acquire берёт столько задач, сколько свободно слотов, и запускает их, не дожидаясь
// завершения: медленная задача занимает только свой слот.
func (w *Worker[T]) acquire(ctx context.Context, num int, slots *slots) error {
	free := slots.free()
	if free == 0 {
		return nil
	}

	jobs, err := w.store.Acquire(ctx, free, w.cfg.LeaseDuration, uuid.New())
	if err != nil {
		return fmt.Errorf("acquire: %w", err)
	}
	if len(jobs) == 0 {
		return nil
	}

	w.obs.Acquired(ctx, len(jobs))

	for _, job := range jobs {
		job.Worker = num
		slots.run(func() { w.process(ctx, job) })
	}

	return nil
}

func (w *Worker[T]) process(ctx context.Context, job Job[T]) {
	ctx = w.obs.JobStarted(ctx, job)

	jobCtx, cancelJob := context.WithTimeout(ctx, w.cfg.JobTimeout)
	err := w.handle(jobCtx, job)
	interrupted := jobCtx.Err() != nil
	cancelJob()

	shuttingDown := ctx.Err() != nil

	settleCtx, cancelSettle := context.WithTimeout(context.WithoutCancel(ctx), w.cfg.SettleTimeout)
	defer cancelSettle()

	result := w.settle(settleCtx, job, err, interrupted, shuttingDown)
	w.obs.JobFinished(settleCtx, job, result)
}

func (w *Worker[T]) handle(ctx context.Context, job Job[T]) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%w: %v", ErrPanic, r)
		}
	}()

	return w.handler.Handle(ctx, job)
}

// settle записывает результат. Ошибка, вернувшаяся после отмены контекста задачи, —
// не вина задачи: при остановке задача возвращается в очередь без расхода попытки,
// при таймауте повторяется как обычная ошибка.
func (w *Worker[T]) settle(ctx context.Context, job Job[T], err error, interrupted, shuttingDown bool) Result {
	if err == nil {
		ok, storeErr := w.store.Complete(ctx, job)
		return Result{Outcome: OutcomeCompleted, LostLease: !ok && storeErr == nil, StoreErr: storeErr}
	}

	if interrupted {
		if shuttingDown {
			ok, storeErr := w.store.Release(ctx, job)
			return Result{Outcome: OutcomeReleased, Err: err, LostLease: !ok && storeErr == nil, StoreErr: storeErr}
		}

		err = fmt.Errorf("%w (%s): %w", ErrJobTimeout, w.cfg.JobTimeout, err)

		return w.retryOrFail(ctx, job, err)
	}

	if delay, ok := SnoozeDelay(err); ok {
		ok, storeErr := w.store.Snooze(ctx, job, w.now().Add(delay), err)
		return Result{Outcome: OutcomeSnoozed, Err: err, LostLease: !ok && storeErr == nil, StoreErr: storeErr}
	}

	if IsPermanent(err) {
		ok, storeErr := w.store.Fail(ctx, job, err)
		return Result{Outcome: OutcomeFailed, Err: err, LostLease: !ok && storeErr == nil, StoreErr: storeErr}
	}

	return w.retryOrFail(ctx, job, err)
}

func (w *Worker[T]) retryOrFail(ctx context.Context, job Job[T], err error) Result {
	decision := w.retry.Next(job.Attempts, job.MaxAttempts, w.now(), err)

	if decision.Fail {
		ok, storeErr := w.store.Fail(ctx, job, err)
		return Result{Outcome: OutcomeFailed, Err: err, Retry: decision, LostLease: !ok && storeErr == nil, StoreErr: storeErr}
	}

	ok, storeErr := w.store.Retry(ctx, job, decision, err)

	return Result{Outcome: OutcomeRetried, Err: err, Retry: decision, LostLease: !ok && storeErr == nil, StoreErr: storeErr}
}

func (w *Worker[T]) runRescuer(ctx context.Context) {
	ticker := time.NewTicker(w.cfg.RescueInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		count, err := w.store.Rescue(ctx)
		if err != nil {
			if ctx.Err() == nil {
				w.obs.Error(ctx, OpRescue, fmt.Errorf("rescue: %w", err))
			}

			continue
		}

		if count > 0 {
			w.obs.Rescued(ctx, count)
		}
	}
}

// slots ограничивает число одновременно выполняемых задач цикла.
type slots struct {
	sem chan struct{}
	wg  sync.WaitGroup
}

func newSlots(size int) *slots {
	return &slots{sem: make(chan struct{}, size)}
}

func (s *slots) free() int {
	return cap(s.sem) - len(s.sem)
}

// run занимает слот и выполняет fn в горутине. Вызывающий не запускает больше free()
// задач, поэтому занятие слота не блокируется.
func (s *slots) run(fn func()) {
	s.sem <- struct{}{}
	s.wg.Add(1)

	go func() {
		defer func() {
			<-s.sem
			s.wg.Done()
		}()

		fn()
	}()
}

func (s *slots) wait() {
	s.wg.Wait()
}
