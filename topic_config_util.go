package kafka_dao

func checkBreakingChange(orig, target TopicConfig) (isBreaking bool) {
	return orig.Name != target.Name ||
		orig.ReplicationFactor != target.ReplicationFactor ||
		orig.Partitions != target.Partitions
}
