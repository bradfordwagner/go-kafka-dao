package kafka_dao

import "github.com/Shopify/sarama"

func (d *daoImpl) GetTopicConfig(topic string) (ok bool, tc TopicConfig, err error) {
	tc.Name = topic
	// setup connection
	err = d.buildAdminConnection()
	if err != nil {
		return
	}

	tc.ACLs, err = d.GetTopicACLs(topic)
	if err != nil {
		return
	}

	topics, err := d.admin.Get().ListTopics()
	if err != nil {
		return
	}

	details, ok := topics[topic]
	if !ok {
		return
	}
	tc.Partitions, tc.ReplicationFactor = int(details.NumPartitions), int(details.ReplicationFactor)

	// parse to a map so in future other configurations can be parsed
	topicConfig, err := d.admin.Get().DescribeConfig(sarama.ConfigResource{
		Type:        sarama.TopicResource,
		Name:        topic,
		ConfigNames: []string{"retention.ms"},
	})
	configs := make(map[string]string, len(topicConfig))
	for _, entry := range topicConfig {
		configs[entry.Name] = entry.Value
	}
	tc.Config.RetentionMS, _ = configs["retention.ms"]

	return
}
