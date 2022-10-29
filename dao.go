package kafka_dao

type DAO interface {
	AlterTopicRetentionMillis(topicName, retention string) (err error)
	DeleteTopicLatestRecords(topicName string) (err error)
	GetBrokerIDs() (brokerIds []int32, err error)
	GetTopicNewestOffsets(topicName string) (partitionsToOffsets map[int32]int64, err error)
	GetTopicPartitions(topicName string) (partitions []int32, err error)
}
