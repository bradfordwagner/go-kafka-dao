package kafka_dao

import (
	"github.com/Shopify/sarama"
	"github.com/bradfordwagner/go-kafka-dao/mocks/pkg/mock_sarama"
	"github.com/golang/mock/gomock"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/atomic"
)

var _ = Describe("BuildAdminConnection", func() {
	It("test", func() { Expect(true).To(BeTrue()) })
	// setup controller for mock invocations
	var ctrl *gomock.Controller
	BeforeEach(func() { ctrl = gomock.NewController(GinkgoT()) })
	AfterEach(func() { ctrl.Finish() })

	type args struct {
		initialAdmin sarama.ClusterAdmin
	}
	type harness struct {
		invocations *atomic.Int32
		di          *daoImpl
	}
	stage := func(a args) *harness {
		h := &harness{
			invocations: atomic.NewInt32(0),
		}
		f := func() sarama.ClusterAdmin {
			h.invocations.Inc()
			return mock_sarama.NewMockClusterAdmin(ctrl)
		}
		di := New("", OptionsAdminBuilder(f)).(*daoImpl)
		di.admin.Set(a.initialAdmin)
		h.di = di
		return h
	}

	It("has an admin already so does not set it", func() {
		admin := mock_sarama.NewMockClusterAdmin(ctrl)
		h := stage(args{initialAdmin: admin})
		Expect(h.invocations.Load()).To(Equal(int32(0)))
		Expect(h.di.admin.Get()).To(Equal(admin))
		h.di.buildAdminConnection()
		Expect(h.invocations.Load()).To(Equal(int32(0)))
		Expect(h.di.admin.Get()).To(Equal(admin))
	})
	It("needs an admin client so builds it", func() {
		h := stage(args{})
		Expect(h.invocations.Load()).To(Equal(int32(0)))
		Expect(h.di.admin.Get()).To(BeNil())
		h.di.buildAdminConnection()
		Expect(h.invocations.Load()).To(Equal(int32(1)))
		Expect(h.di.admin.Get()).To(Not(BeNil()))

		// invoking again does not load a new client
		h.di.buildAdminConnection()
		Expect(h.invocations.Load()).To(Equal(int32(1)))
		Expect(h.di.admin.Get()).To(Not(BeNil()))
	})
})
