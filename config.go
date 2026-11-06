package queueworker

import (
	"errors"
	"time"
)

const (
	DefaultWorkers        = 1
	DefaultConcurrency    = 10
	DefaultPollInterval   = 500 * time.Millisecond
	DefaultLeaseDuration  = 90 * time.Second
	DefaultJobTimeout     = 60 * time.Second
	DefaultRescueInterval = 30 * time.Second
	DefaultSettleTimeout  = 10 * time.Second
)

// Config — параметры воркера. Нулевые значения заменяются значениями по умолчанию.
type Config struct {
	// Workers — число независимых циклов захвата.
	Workers int
	// Concurrency — сколько задач один цикл выполняет одновременно. Захват берёт
	// столько задач, сколько слотов свободно, и не ждёт завершения остальных.
	Concurrency int
	// PollInterval — интервал захвата; за тик забираются все освободившиеся слоты.
	PollInterval time.Duration
	// LeaseDuration — срок аренды. Аренда не продлевается.
	LeaseDuration time.Duration
	// JobTimeout — максимальная длительность задачи; должна быть меньше LeaseDuration,
	// иначе rescuer вернёт в очередь ещё выполняющуюся задачу.
	JobTimeout time.Duration
	// RescueInterval — как часто rescuer ищет задачи с истёкшей арендой.
	RescueInterval time.Duration
	// SettleTimeout ограничивает запись результата; запись не прерывается остановкой.
	SettleTimeout time.Duration
}

func (c Config) withDefaults() Config {
	if c.Workers == 0 {
		c.Workers = DefaultWorkers
	}
	if c.Concurrency == 0 {
		c.Concurrency = DefaultConcurrency
	}
	if c.PollInterval == 0 {
		c.PollInterval = DefaultPollInterval
	}
	if c.LeaseDuration == 0 {
		c.LeaseDuration = DefaultLeaseDuration
	}
	if c.JobTimeout == 0 {
		// Задача должна укладываться в аренду с запасом.
		c.JobTimeout = min(DefaultJobTimeout, c.LeaseDuration*2/3)
	}
	if c.RescueInterval == 0 {
		c.RescueInterval = DefaultRescueInterval
	}
	if c.SettleTimeout == 0 {
		c.SettleTimeout = DefaultSettleTimeout
	}

	return c
}

func (c Config) validate() error {
	switch {
	case c.Workers < 0, c.Concurrency < 0:
		return errors.New("queueworker: workers and concurrency must be positive")
	case c.PollInterval < 0, c.LeaseDuration < 0, c.RescueInterval < 0, c.SettleTimeout < 0, c.JobTimeout < 0:
		return errors.New("queueworker: intervals must be positive")
	case c.JobTimeout >= c.LeaseDuration:
		return errors.New("queueworker: job timeout must be less than lease duration")
	}

	return nil
}
