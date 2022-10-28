package template

import "time"

// Config
type Config struct {
	numWorkers int
	tick       time.Duration
}

type Option func(config *Config)

func newDefaultConfig() *Config {
	return &Config{
		numWorkers: 15,
		tick:       time.Second * 5,
	}
}

func OptionWorkerRoutines(numWorkers int) Option {
	return func(config *Config) {
		config.numWorkers = numWorkers
	}
}

func OptionTick(tick time.Duration) Option {
	return func(config *Config) {
		config.tick = tick
	}
}
