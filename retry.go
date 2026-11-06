package queueworker

import (
	"math/rand/v2"
	"time"
)

// RetryDecision — что делать с задачей после ошибки.
type RetryDecision struct {
	// Attempts — число попыток, которое запишется в задачу.
	Attempts int
	// At — когда задачу можно брать снова.
	At time.Time
	// Fail — попытки кончились, задача завершается окончательной ошибкой.
	Fail bool
	// Exhausted — лимит попыток достигнут, но политика решила продолжать (например,
	// редкие повторы вместо окончательной ошибки). Только для наблюдения.
	Exhausted bool
}

// RetryPolicy решает судьбу задачи после ошибки.
type RetryPolicy interface {
	Next(attempts, maxAttempts int, now time.Time, cause error) RetryDecision
}

// RetryPolicyFunc позволяет использовать функцию как RetryPolicy.
type RetryPolicyFunc func(attempts, maxAttempts int, now time.Time, cause error) RetryDecision

func (f RetryPolicyFunc) Next(attempts, maxAttempts int, now time.Time, cause error) RetryDecision {
	return f(attempts, maxAttempts, now, cause)
}

// QuadraticBackoff — повтор через (n+1)² × Base + случайная добавка до Jitter, где n —
// номер попытки; после MaxAttempts — окончательная ошибка.
type QuadraticBackoff struct {
	Base   time.Duration
	Jitter time.Duration
}

func (b QuadraticBackoff) Next(attempts, maxAttempts int, now time.Time, _ error) RetryDecision {
	n := attempts + 1
	if maxAttempts > 0 && n >= maxAttempts {
		return RetryDecision{Attempts: n, At: now, Fail: true}
	}

	delay := time.Duration((n+1)*(n+1)) * b.Base

	return RetryDecision{Attempts: n, At: now.Add(delay + jitter(b.Jitter))}
}

func jitter(maxDur time.Duration) time.Duration {
	if maxDur <= 0 {
		return 0
	}

	return rand.N(maxDur + 1) //nolint:gosec // разброс задержки, не криптография
}
