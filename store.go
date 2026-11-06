package queueworker

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// Store — хранилище очереди.
//
// Контракт:
//   - Acquire атомарно захватывает до limit доступных задач, ставит им token и срок
//     аренды now+lease и возвращает их; задача не должна достаться двум захватам;
//   - Complete, Retry, Fail, Snooze, Release меняют задачу, только если она всё ещё
//     захвачена этим токеном (job.Token), и возвращают false, если владение потеряно;
//   - Rescue возвращает в очередь задачи с истёкшей арендой; условие истечения
//     проверяется в самом обновлении, а не в предварительной выборке.
type Store[T any] interface {
	Acquire(ctx context.Context, limit int, lease time.Duration, token uuid.UUID) ([]Job[T], error)
	// Complete — задача выполнена.
	Complete(ctx context.Context, job Job[T]) (bool, error)
	// Retry возвращает задачу в очередь с новым числом попыток и временем запуска.
	Retry(ctx context.Context, job Job[T], decision RetryDecision, cause error) (bool, error)
	// Fail — окончательная ошибка.
	Fail(ctx context.Context, job Job[T], cause error) (bool, error)
	// Snooze откладывает задачу до at без изменения числа попыток.
	Snooze(ctx context.Context, job Job[T], at time.Time, cause error) (bool, error)
	// Release возвращает задачу в очередь сразу, без изменения числа попыток.
	Release(ctx context.Context, job Job[T]) (bool, error)
	Rescue(ctx context.Context) (int64, error)
}
