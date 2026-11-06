package queueworker

import "context"

// Outcome — чем закончилась обработка задачи.
type Outcome int

const (
	OutcomeCompleted Outcome = iota
	OutcomeRetried
	OutcomeFailed
	OutcomeSnoozed
	// OutcomeReleased — задача прервана остановкой воркера и возвращена в очередь
	// без расхода попытки.
	OutcomeReleased
)

func (o Outcome) String() string {
	switch o {
	case OutcomeCompleted:
		return "completed"
	case OutcomeRetried:
		return "retried"
	case OutcomeFailed:
		return "failed"
	case OutcomeSnoozed:
		return "snoozed"
	case OutcomeReleased:
		return "released"
	default:
		return "unknown"
	}
}

// Result — итог обработки задачи для наблюдателя.
type Result struct {
	Outcome Outcome
	// Err — ошибка обработчика (для Completed — nil). При таймауте обёрнута в
	// ErrJobTimeout, при панике — в ErrPanic.
	Err error
	// Retry заполнено для OutcomeRetried и для OutcomeFailed по исчерпанию попыток.
	Retry RetryDecision
	// LostLease — хранилище не записало результат: задача уже не за этим токеном.
	LostLease bool
	// StoreErr — ошибка записи результата в хранилище.
	StoreErr error
}

// Op — фоновая операция воркера.
type Op string

const (
	OpAcquire Op = "acquire"
	OpRescue  Op = "rescue"
)

// Observer получает события воркера: точка подключения логов, метрик и трейсинга.
// Методы вызываются конкурентно из разных горутин.
type Observer[T any] interface {
	// JobStarted вызывается перед обработчиком. Возвращённый контекст передаётся
	// обработчику и в JobFinished (например, со span'ом).
	JobStarted(ctx context.Context, job Job[T]) context.Context
	// JobFinished вызывается после записи результата; ctx не отменяется остановкой.
	JobFinished(ctx context.Context, job Job[T], result Result)
	Acquired(ctx context.Context, count int)
	Rescued(ctx context.Context, count int64)
	Error(ctx context.Context, op Op, err error)
}

// NopObserver ничего не делает.
type NopObserver[T any] struct{}

func (NopObserver[T]) JobStarted(ctx context.Context, _ Job[T]) context.Context { return ctx }
func (NopObserver[T]) JobFinished(context.Context, Job[T], Result)              {}
func (NopObserver[T]) Acquired(context.Context, int)                            {}
func (NopObserver[T]) Rescued(context.Context, int64)                           {}
func (NopObserver[T]) Error(context.Context, Op, error)                         {}
