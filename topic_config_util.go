package kafka_dao

import (
	"github.com/Shopify/sarama"
	"github.com/bradfordwagner/go-util"
)

func checkBreakingChange(orig, target TopicConfig) (isBreaking bool) {
	return orig.Name != target.Name ||
		orig.ReplicationFactor != target.ReplicationFactor ||
		orig.Partitions != target.Partitions
}

func (t TopicConfig) ConvertToSaramaTopicDetails() *sarama.TopicDetail {
	configEntries := make(map[string]*string)
	if t.Config.RetentionMS != "" {
		configEntries["retention.ms"] = bwutil.Pointer(t.Config.RetentionMS)
	}
	return &sarama.TopicDetail{
		NumPartitions:     int32(t.Partitions),
		ReplicationFactor: int16(t.ReplicationFactor),
		ReplicaAssignment: nil,
		ConfigEntries:     configEntries,
	}
}
