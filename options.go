package kafka_dao

import "github.com/Shopify/sarama"

var defaultSaramaVersion = sarama.V2_8_0_0

// Config - setup configuration for dao impl setup
type Config struct {
	brokers string
	version sarama.KafkaVersion
}

type Option func(config *Config)

func newDefaultConfig(brokers string) *Config {
	return &Config{
		brokers: brokers,
		version: defaultSaramaVersion,
	}
}

// OptionKafkaVersion - configure kafka version
func OptionKafkaVersion(version sarama.KafkaVersion) Option {
	return func(config *Config) {
		config.version = version
	}
}
