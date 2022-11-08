package kafka_dao

import (
	"errors"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/bradfordwagner/go-util"
	"strings"
)

func (t TopicConfig) checkBreakingChange(target TopicConfig) (isBreaking bool, err error) {
	var errs []string
	if t.Name != target.Name {
		errs = append(errs, fmt.Sprintf("name %s!=%s", target.Name, t.Name))
	}
	if t.ReplicationFactor != target.ReplicationFactor {
		errs = append(errs, fmt.Sprintf("replication_factor %d!=%d", target.ReplicationFactor, t.ReplicationFactor))
	}
	if t.Partitions != target.Partitions {
		errs = append(errs, fmt.Sprintf("partitions %d!=%d", target.Partitions, t.Partitions))
	}

	isBreaking = len(errs) > 0
	if isBreaking {
		err = errors.New(fmt.Sprintf("topic=%s contains breaking changes (target!=orig): %s", t.Name, strings.Join(errs, ",")))
	}
	return
}

func (t TopicConfig) ConvertToSaramaTopicDetails() *sarama.TopicDetail {
	configEntries := t.Config.ToConfigMap()
	return &sarama.TopicDetail{
		NumPartitions:     int32(t.Partitions),
		ReplicationFactor: int16(t.ReplicationFactor),
		ReplicaAssignment: nil,
		ConfigEntries:     configEntries,
	}
}

func (tcd TopicConfigDetails) ToConfigMap() map[string]*string {
	configEntries := make(map[string]*string)
	if tcd.RetentionMS != "" {
		configEntries["retention.ms"] = bwutil.Pointer(tcd.RetentionMS)
	}
	return configEntries
}
