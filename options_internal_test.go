package kafka_dao

import (
	"github.com/Shopify/sarama"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Options", func() {
	brokers := "abc:1234"
	It("Default Brokers", func() {
		c := newDefaultConfig(brokers)
		Expect(c.brokers).To(Equal(brokers))
		Expect(c.version).To(Equal(defaultSaramaVersion))
	})

	It("Sets Option Kafka Version", func() {
		c := newDefaultConfig(brokers)
		// check defaults
		Expect(c.brokers).To(Equal(brokers))
		Expect(c.version).To(Equal(defaultSaramaVersion))

		// override defaults
		expectedVersion := sarama.V0_10_0_0
		OptionKafkaVersion(expectedVersion)(c)
		Expect(c.version).To(Equal(expectedVersion))
	})
})
