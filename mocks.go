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
