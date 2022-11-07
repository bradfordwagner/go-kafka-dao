package kafka_dao

import (
	"errors"
	"github.com/Shopify/sarama"
	"github.com/bradfordwagner/go-kafka-dao/mocks/pkg/mock_sarama"
	bwutil "github.com/bradfordwagner/go-util"
	"github.com/golang/mock/gomock"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("GetTopicConfig", func() {
	var topic = "hi_friends"
	var ctrl *gomock.Controller
	var admin *mock_sarama.MockClusterAdmin
	var d *daoImpl
	BeforeEach(func() {
		ctrl = gomock.NewController(GinkgoT())
		admin = mock_sarama.NewMockClusterAdmin(ctrl)
		dg := New("brokers")
		d = dg.(*daoImpl)
		d.admin = bwutil.NewLockableWithValue[sarama.ClusterAdmin](admin)
	})
	AfterEach(func() {
		Expect(admin).ShouldNot(BeNil())
		Expect(d).ShouldNot(BeNil())
		ctrl.Finish()
	})

	It("fails to initialize an admin connection", func() {
		err := errors.New("expected")
		d.admin = bwutil.NewLockable[sarama.ClusterAdmin]() // force admin buidler function to run
		d.config = newDefaultConfig("abcd")
		d.config.adminBuilder = func(brokers string, version sarama.KafkaVersion) (sarama.ClusterAdmin, error) {
			return nil, err
		}
		ok, tc, resErr := d.GetTopicConfig(topic)
		Expect(ok).To(BeFalse())
		Expect(tc).To(Equal(TopicConfig{}))
		Expect(resErr).To(Equal(err))
	})

	type argsRes struct {
		ok  bool
		tc  TopicConfig
		err error
	}

	type argsListTopics struct {
		topics []map[string]sarama.TopicDetail
	}
	type args struct {
		acls *mockGetTopicACLs
		res  argsRes
	}
	var test = func(a args) {
		d.getTopicACLs = a.acls

		ok, tc, err := d.GetTopicConfig(topic)
		Expect(ok).To(Equal(a.res.ok))
		Expect(tc).To(Equal(a.res.tc))
		if a.res.err != nil {
			Expect(err).To(Equal(a.res.err))
		} else {
			Expect(err).ShouldNot(HaveOccurred())
		}
	}

	It("fails to fetch acls", func() {
		err := errors.New("expected")
		test(args{
			acls: newMockGetTopicACLs(
				ACLs{},
				nil,
			),
			res: argsRes{
				err: err,
			},
		})
	})

})
