package kafka_dao

import "github.com/Shopify/sarama"

var defaultSaramaVersion = sarama.V2_8_0_0

// OptionAdminBuilderFunc -
type OptionAdminBuilderFunc = func(brokers string, version sarama.KafkaVersion) (sarama.ClusterAdmin, error)

// config - setup configuration for dao impl setup
type config struct {
	brokers      string
	version      sarama.KafkaVersion
	adminBuilder OptionAdminBuilderFunc
}

type Option func(config *config)

func newDefaultConfig(brokers string) *config {
	return &config{
		brokers: brokers,
		version: defaultSaramaVersion,
	}
}

// OptionKafkaVersion - configure kafka version
func OptionKafkaVersion(version sarama.KafkaVersion) Option {
	return func(config *config) {
		config.version = version
	}
}

// OptionsAdminBuilder - helps to build the admin client also useful for mocking
func OptionsAdminBuilder(f OptionAdminBuilderFunc) Option {
	return func(config *config) {
		config.adminBuilder = f
	}
}
