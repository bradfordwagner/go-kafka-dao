package kafka_dao

import "go.uber.org/atomic"

//go:generate mockgen -destination=mocks/pkg/mock_sarama/module.go -package=mock_sarama github.com/Shopify/sarama ClusterAdmin,Client

// mockGetTopicACLs - used for testing other parts of the dao which invoke get topic acls
type mockGetTopicACLs struct {
	invoked *atomic.Int32
	acls    ACLs
	err     error
}

func newMockGetTopicACLs(acls ACLs, err error) *mockGetTopicACLs {
	return &mockGetTopicACLs{
		invoked: atomic.NewInt32(0),
		acls:    acls,
		err:     err,
	}
}

func (m *mockGetTopicACLs) GetTopicACLs(topic string) (acls ACLs, err error) {
	m.invoked.Inc()
	return m.acls, m.err
}

var _ GetTopicACLs = (*mockGetTopicACLs)(nil)

// end - mockGetTopicACLs

// mockGetTopicConfig - used for testing other parts of the dao which invoke get topic acls
type mockGetTopicConfig struct {
	invoked     *atomic.Int32
	ok          bool
	topicConfig TopicConfig
	err         error
}

func newMockGetTopicConfig(ok bool, topicConfig TopicConfig, err error) *mockGetTopicConfig {
	return &mockGetTopicConfig{
		invoked:     atomic.NewInt32(0),
		ok:          ok,
		topicConfig: topicConfig,
		err:         err,
	}
}

func (m *mockGetTopicConfig) GetTopicConfig(topic string) (exists bool, tc TopicConfig, err error) {
	m.invoked.Inc()
	return m.ok, m.topicConfig, m.err
}

var _ GetTopicConfig = (*mockGetTopicConfig)(nil)

// end mockGetTopicConfig

// mockReconcileACLs - used for testing other parts of the dao which invoke get topic acls
type mockReconcileACLs struct {
	invoked *atomic.Int32
	err     error
}

func newMockReconcileACLs(err error) *mockReconcileACLs {
	return &mockReconcileACLs{
		invoked: atomic.NewInt32(0),
		err:     err,
	}
}

func (m *mockReconcileACLs) reconcileACLs(orig, target TopicConfig) (err error) {
	m.invoked.Inc()
	return m.err
}

var _ reconcileACLs = (*mockReconcileACLs)(nil)

// end mockGetTopicConfig
