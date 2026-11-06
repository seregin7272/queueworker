# queueworker

Фоновые задачи из очереди в базе данных для Go. Движок не зависит от хранилища;
готовое хранилище на PostgreSQL (pgx) — пакет `pgstore`.

## Модель

Аренда задачи **без продления** с **токеном на каждый захват**:

```
захват:   token = uuid.New(), lease_until = now + LeaseDuration
задача:   ограничена JobTimeout < LeaseDuration → заканчивается раньше аренды
запись:   WHERE id = $id AND locked_by = $token → опоздавшая попытка получает 0 строк
rescuer:  WHERE state = 'running' AND lease_until < now() → вернуть в очередь
```

- Захватывается столько задач, сколько у цикла свободных слотов (`Concurrency`);
  медленная задача занимает только свой слот.
- Ошибка, вернувшаяся после отмены контекста задачи, — не вина задачи: при
  остановке задача возвращается в очередь без расхода попытки (`OutcomeReleased`),
  при таймауте — повторяется с `ErrJobTimeout`.
- Результат записывается с контекстом, отвязанным от остановки (`SettleTimeout`).
- Паника обработчика превращается в повтор (`ErrPanic`).

## Использование

```go
// Схема: pgstore.Schema — применить миграцией приложения.

// Постановка, в том числе внутри транзакции приложения:
_, err := pgstore.Insert(ctx, tx, "emails", Email{To: "a@b.c"}, pgstore.InsertOptions{MaxAttempts: 5})

store, _ := pgstore.New[Email](pool, "emails")

w, err := queueworker.New[Email](queueworker.Config{
	Workers:       2,
	Concurrency:   10,
	LeaseDuration: 90 * time.Second,
	JobTimeout:    60 * time.Second,
}, store, queueworker.HandlerFunc[Email](func(ctx context.Context, job queueworker.Job[Email]) error {
	return send(ctx, job.Data)
}))

w.Run(ctx) // до отмены ctx; дожидается задач в работе
```

Результат обработчика:

| Возврат | Итог |
|---|---|
| `nil` | выполнена, задача удаляется |
| `queueworker.Permanent(err)` | окончательная ошибка (`pgstore`: перенос в `queueworker_failed_jobs`) |
| `queueworker.Snooze(d, err)` | отложить на `d` без расхода попытки (например, circuit breaker открыт) |
| другая ошибка | повтор по `RetryPolicy` (по умолчанию `QuadraticBackoff`) |

## Расширение

- **Своя таблица** — реализовать `queueworker.Store[T]`. Все записи результата обязаны
  проверять `job.Token`, а `Rescue` — условие истечения аренды прямо в `UPDATE`.
- **Политика повторов** — `WithRetryPolicy`; `RetryDecision.Fail` завершает задачу,
  `Exhausted` — для наблюдения (лимит достигнут, но повторы продолжаются).
- **Логи, метрики, трейсинг** — `WithObserver`: `JobStarted` может положить span в
  контекст, `JobFinished` получает `Result` с исходом, ошибкой и признаком потери
  владения.

## Ограничения

- Аренда не продлевается: задачи должны укладываться в `JobTimeout`. Для долгих задач
  разбивайте работу или увеличивайте `LeaseDuration` вместе с `JobTimeout`.
- Задача, игнорирующая отмену контекста, продолжит работать после таймаута; rescuer
  вернёт её в очередь через `LeaseDuration`, а запись её результата отсечёт токен.
  Внешние эффекты (HTTP, сообщения) делайте идемпотентными.
- Падение процесса не считается попыткой.

## Тесты

```
go test -race ./...                    # движок
cd integration && go test -race ./...  # pgstore на PostgreSQL (testcontainers, нужен Docker)
```
