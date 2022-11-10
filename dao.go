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
	Config            TopicConfigDetails
	ACLs              ACLs
}

type TopicConfigDetails struct {
	// retention time for topic in ms, default=none
	RetentionMS string
}

type ACLs struct {
	// Principals to allow Writes for
	Writes *bwutil.Set[string]
	// Principals to allow Reads for
	Reads *bwutil.Set[string]
}

// DAO - Data Access Object
type DAO interface {
	buildAdminConnection
	GetTopicACLs
	GetTopicConfig
	reconcileACLs
	// ListTopics - lists topic configurations including ACLs
	//ListTopics() (tc map[string]TopicConfig, err error)

	// UpsertTopic - upserts a topic configuration to kafka. if some immutable fields have been changed then will return error
	UpsertTopic(t TopicConfig) (err error)

	// DeleteTopic - deletes a topic configuration from kafka
	//DeleteTopic(t TopicConfig) (err error)

	//AlterTopicRetentionMillis(topicName, retention string) (err error)
	//DeleteTopicLatestRecords(topicName string) (err error)
	//GetBrokerIDs() (brokerIds []int32, err error)
	//GetTopicNewestOffsets(topicName string) (partitionsToOffsets map[int32]int64, err error)
	//GetTopicPartitions(topicName string) (partitions []int32, err error)
}

// GetTopicACLs - returns topic acls
type GetTopicACLs interface {
	GetTopicACLs(topic string) (acls ACLs, err error)
}

// GetTopicConfig - returns topic configuration
type GetTopicConfig interface {
	GetTopicConfig(topic string) (exists bool, tc TopicConfig, err error)
}

// buildAdminConnection - sets up the admin connection to kafka
type buildAdminConnection interface {
	buildAdminConnection() (err error)
}

type reconcileACLs interface {
	reconcileACLs(orig, target TopicConfig) (err error)
}

// New - Creates a new implementation of the kafka DAO
func New(brokers string, options ...Option) DAO {
	c := newDefaultConfig(brokers)
	for _, option := range options {
		option(c)
	}

	self := &daoImpl{
		config: c,
		admin:  bwutil.NewLockable[sarama.ClusterAdmin](),
	}
	// allows internal tests to override
	self.buildAdminConn = self
	self.getTopicACLs = self
	self.reconcileACLsComponent = self
	self.getTopicConfigComponent = self
	return self
}

// daoImpl - implementation of the DAO interface
type daoImpl struct {
	config                  *config
	admin                   *bwutil.Lockable[sarama.ClusterAdmin]
	getTopicACLs            GetTopicACLs
	buildAdminConn          buildAdminConnection
	reconcileACLsComponent  reconcileACLs
	getTopicConfigComponent GetTopicConfig
}

// enforce interface
var _ DAO = (*daoImpl)(nil)

func newACLS() ACLs {
	return ACLs{
		Writes: bwutil.NewSet[string](),
		Reads:  bwutil.NewSet[string](),
	}
}
