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

var _ = FDescribe("GetTopicConfig", func() {
	var topic = "hi_friends"
	var ctrl *gomock.Controller
	var admin *mock_sarama.MockClusterAdmin
	var d *daoImpl
	BeforeEach(func() {
		ctrl = gomock.NewController(GinkgoT())
		admin = mock_sarama.NewMockClusterAdmin(ctrl)
		d = &daoImpl{
			config: nil,
			admin:  bwutil.NewLockableWithValue[sarama.ClusterAdmin](admin),
		}
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
})
