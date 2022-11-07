package kafka_dao

import (
	"github.com/Shopify/sarama"
	"github.com/bradfordwagner/go-kafka-dao/mocks/pkg/mock_sarama"
	bwutil "github.com/bradfordwagner/go-util"
	"github.com/golang/mock/gomock"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = FDescribe("GetTopicAcls", func() {
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

	type argAcl struct {
		v   []string
		err error
	}
	type argRes struct {
		acls ACLs
		err  error
	}

	type args struct {
		reads  argAcl
		writes argAcl
		res    argRes
	}

	var convertToResourceACLS = func(strs []string) (res []sarama.ResourceAcls) {
		var acls []*sarama.Acl
		for _, str := range strs {
			acls = append(acls, &sarama.Acl{
				Principal: str,
			})
		}
		res = []sarama.ResourceAcls{
			{
				Resource: sarama.Resource{},
				Acls:     acls,
			},
		}
		return
	}

	var test = func(a args) {
		// return no acls, no error
		admin.EXPECT().ListAcls(sarama.AclFilter{
			ResourceName:              bwutil.Pointer(topic),
			Operation:                 sarama.AclOperationRead,
			ResourceType:              sarama.AclResourceTopic,
			ResourcePatternTypeFilter: sarama.AclPatternLiteral,
			PermissionType:            sarama.AclPermissionAllow,
		}).Return(convertToResourceACLS(a.reads.v), a.reads.err)
		admin.EXPECT().ListAcls(sarama.AclFilter{
			ResourceName: bwutil.Pointer(topic),
			// Operation is the diff between this and prev
			Operation:                 sarama.AclOperationWrite,
			ResourceType:              sarama.AclResourceTopic,
			ResourcePatternTypeFilter: sarama.AclPatternLiteral,
			PermissionType:            sarama.AclPermissionAllow,
		}).Return(convertToResourceACLS(a.writes.v), a.writes.err)

		// run it and check
		acls, err := d.GetTopicACLs(topic)
		Expect(acls).Should(Equal(a.res.acls))
		if a.res.err == nil {
			Expect(err).To(BeNil())
		} else {
			Expect(err).ToNot(BeNil())
		}
	}

	It("has no acls", func() {
		test(args{
			reads:  argAcl{},
			writes: argAcl{},
			res:    argRes{acls: newACLS()},
		})
	})

})
