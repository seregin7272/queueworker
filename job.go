// Package queueworker выполняет фоновые задачи из очереди в базе данных.
//
// Модель владения — аренда задачи без продления с токеном на каждый захват:
//
//   - захват ставит задаче токен и срок аренды;
//   - задача ограничена Config.JobTimeout, который меньше срока аренды, поэтому
//     заканчивается раньше, чем аренда истечёт;
//   - все записи результата проверяют токен: опоздавшая попытка ничего не запишет;
//   - rescuer возвращает в очередь задачи с истёкшей арендой (воркер упал или завис).
//
// Хранилище подключается через Store; готовая реализация на PostgreSQL — пакет pgstore.
package queueworker

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// Job — захваченная задача.
type Job[T any] struct {
	ID int64
	// Attempts — сколько попыток уже израсходовано до текущей.
	Attempts    int
	MaxAttempts int
	// Token — токен захвата. Новый при каждом захвате; хранилище обязано проверять
	// его при каждой записи результата.
	Token uuid.UUID
	// Worker — номер цикла захвата (с 1), взявшего задачу; для логов и трейсинга.
	Worker int
	Data   T
}

// Handler выполняет задачу. Возвращаемое значение решает её судьбу:
//
//   - nil — задача выполнена;
//   - Permanent(err) — окончательная ошибка, без повторов;
//   - Snooze(d, err) — отложить на d без расхода попытки (например, при открытом
//     circuit breaker);
//   - любая другая ошибка — повтор по RetryPolicy.
//
// Контекст отменяется по Config.JobTimeout и при остановке воркера. Ошибка, вернувшаяся
// после отмены, не считается ошибкой задачи: при остановке задача возвращается в
// очередь без расхода попытки, при таймауте — повторяется с ErrJobTimeout.
type Handler[T any] interface {
	Handle(ctx context.Context, job Job[T]) error
}

// HandlerFunc позволяет использовать функцию как Handler.
type HandlerFunc[T any] func(ctx context.Context, job Job[T]) error

func (f HandlerFunc[T]) Handle(ctx context.Context, job Job[T]) error {
	return f(ctx, job)
}

// ErrJobTimeout оборачивает ошибку задачи, прерванной по Config.JobTimeout.
var ErrJobTimeout = errors.New("queueworker: job timeout exceeded")

// ErrPanic оборачивает панику обработчика; такая задача повторяется.
var ErrPanic = errors.New("queueworker: handler panicked")

type permanentError struct{ err error }

func (e *permanentError) Error() string { return e.err.Error() }
func (e *permanentError) Unwrap() error { return e.err }

// Permanent помечает ошибку как окончательную: задача не повторяется.
func Permanent(err error) error {
	if err == nil {
		err = errors.New("permanent failure")
	}

	return &permanentError{err: err}
}

// IsPermanent сообщает, помечена ли ошибка через Permanent.
func IsPermanent(err error) bool {
	var p *permanentError
	return errors.As(err, &p)
}

type snoozeError struct {
	delay time.Duration
	err   error
}

func (e *snoozeError) Error() string {
	return fmt.Sprintf("snoozed for %s: %v", e.delay, e.err)
}

func (e *snoozeError) Unwrap() error { return e.err }

// Snooze откладывает задачу на delay без расхода попытки.
func Snooze(delay time.Duration, reason error) error {
	if reason == nil {
		reason = errors.New("snoozed")
	}

	return &snoozeError{delay: delay, err: reason}
}

// SnoozeDelay возвращает задержку, если ошибка создана через Snooze.
func SnoozeDelay(err error) (time.Duration, bool) {
	var s *snoozeError
	if errors.As(err, &s) {
		return s.delay, true
	}

	return 0, false
}
