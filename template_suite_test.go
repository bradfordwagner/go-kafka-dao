package kafka_dao

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestKafkaDAO(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Kafka DAO Suite")
}
