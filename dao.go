package kafka_dao

import (
	"github.com/Shopify/sarama"
	"github.com/bradfordwagner/go-util"
)

// TopicConfig - configuration for upserting topics
type TopicConfig struct {
	Name              string
	Partitions        int
	ReplicationFactor int
	Config            struct {
		// retention time for topic in ms, default=none
		RetentionMS string
	}
	ACLs ACLs
}

type ACLs struct {
	// Whether or not to configure acls, if this is disabled then existing acls will be removed from this topic
	Enabled bool
	// Principals to allow Writes for
	Writes *bwutil.Set[string]
	// Principals to allow Reads for
	Reads *bwutil.Set[string]
}

// DAO - Data Access Object
type DAO interface {
	// ListTopics - lists topic configurations including ACLs
	//ListTopics() (tc map[string]TopicConfig, err error)

	GetTopicConfig(topic string) (exists bool, tc TopicConfig, err error)
	GetTopicACLs(topic string) (acls ACLs, err error)

	// UpsertTopic - upserts a topic configuration to kafka. if some immutable fields have been changed then will return error
	//UpsertTopic(t TopicConfig) (err error)

	// DeleteTopic - deletes a topic configuration from kafka
	//DeleteTopic(t TopicConfig) (err error)

	//AlterTopicRetentionMillis(topicName, retention string) (err error)
	//DeleteTopicLatestRecords(topicName string) (err error)
	//GetBrokerIDs() (brokerIds []int32, err error)
	//GetTopicNewestOffsets(topicName string) (partitionsToOffsets map[int32]int64, err error)
	//GetTopicPartitions(topicName string) (partitions []int32, err error)
}

// daoImpl - implementation of the DAO interface
type daoImpl struct {
	config *config
	admin  *bwutil.Lockable[sarama.ClusterAdmin]
}

var _ DAO = (*daoImpl)(nil)

// New - Creates a new implementation of the kafka DAO
func New(brokers string, options ...Option) DAO {
	c := newDefaultConfig(brokers)
	for _, option := range options {
		option(c)
	}

	return &daoImpl{
		config: c,
		admin:  bwutil.NewLockable[sarama.ClusterAdmin](),
	}
}
