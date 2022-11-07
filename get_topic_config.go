package kafka_dao

func (d *daoImpl) GetTopicConfig(topic string) (ok bool, tc TopicConfig, err error) {
	// setup connection
	err = d.buildAdminConn.buildAdminConnection()
	if err != nil {
		return
	}
	tc.Name = topic

	tc.ACLs, err = d.getTopicACLs.GetTopicACLs(topic)
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

	// Configuration details
	retentionMillis, hasRetentionMillis := details.ConfigEntries["retention.ms"]
	if hasRetentionMillis {
		tc.Config.RetentionMS = *retentionMillis
	}

	return
}
