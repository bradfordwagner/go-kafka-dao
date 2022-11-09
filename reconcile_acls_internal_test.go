package kafka_dao

import (
	"github.com/Shopify/sarama"
	"github.com/bradfordwagner/go-kafka-dao/mocks/pkg/mock_sarama"
	"github.com/bradfordwagner/go-util"
	"github.com/golang/mock/gomock"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("ReconcileAcls", func() {
	var ctrl *gomock.Controller
	BeforeEach(func() { ctrl = gomock.NewController(GinkgoT()) })
	AfterEach(func() { ctrl.Finish() })

	type args struct {
		orig, target TopicConfig
	}
	type aclType struct {
		op sarama.AclOperation
		rt sarama.AclResourceType
		p  string
	}
	type deleteFilter struct {
		op sarama.AclOperation
		p  string
	}
	type res struct {
		acls          []aclType
		deleteFilters []deleteFilter
		err           error
	}
	var test = func(a args, r res) {
		admin := mock_sarama.NewMockClusterAdmin(ctrl)
		di := New("brokers").(*daoImpl)
		di.admin = bwutil.NewLockableWithValue[sarama.ClusterAdmin](admin)

		// expect invocations
		//admin.EXPECT().CreateACLs(gomock.Any()).AnyTimes()
		admin.EXPECT().CreateACLs(bwutil.NewMatcherConversionExploderOneOf(r.acls, func(raArr []*sarama.ResourceAcls) (res []aclType) {
			for _, ra := range raArr {
				for _, acl := range ra.Acls {
					res = append(res, aclType{
						op: acl.Operation,
						rt: ra.ResourceType,
						p:  acl.Principal,
					})
				}
			}
			return
		})).AnyTimes() // this invocation is batches, hard to tell how many calls we will have

		admin.EXPECT().DeleteACL(bwutil.NewMatcherConversionOneOf(r.deleteFilters, func(aclFilter sarama.AclFilter) deleteFilter {
			return deleteFilter{
				op: aclFilter.Operation,
				p:  *aclFilter.Principal,
			}
		}), false).Times(len(r.deleteFilters))

		err := di.reconcileACLs(a.orig, a.target)

		if r.err != nil {
			Expect(err).To(Equal(r.err))
		} else {
			Expect(err).ShouldNot(HaveOccurred())
		}
	}

	It("is a new topic", func() {
		test(args{
			orig: TopicConfig{
				ACLs: newACLS(),
			},
			target: TopicConfig{
				Name:              "my_topic",
				Partitions:        3,
				ReplicationFactor: 1,
				Config: TopicConfigDetails{
					RetentionMS: "10",
				},
				ACLs: ACLs{
					Reads:  bwutil.NewSetFromSlice([]string{"read1.test.com"}),
					Writes: bwutil.NewSetFromSlice([]string{}),
				},
			},
		}, res{
			acls: []aclType{
				{
					op: sarama.AclOperationRead,
					rt: sarama.AclResourceTopic,
					p:  "read1.test.com",
				},
				{
					op: sarama.AclOperationRead,
					rt: sarama.AclResourceGroup,
					p:  "read1.test.com",
				},
			},
			deleteFilters: nil,
		})
		test(args{
			orig: TopicConfig{
				ACLs: newACLS(),
			},
			target: TopicConfig{
				Name:              "my_topic",
				Partitions:        3,
				ReplicationFactor: 1,
				Config: TopicConfigDetails{
					RetentionMS: "10",
				},
				ACLs: ACLs{
					Reads:  bwutil.NewSetFromSlice([]string{"read1.test.com"}),
					Writes: bwutil.NewSetFromSlice([]string{"write1.test.com"}),
				},
			},
		}, res{
			acls: []aclType{
				{
					op: sarama.AclOperationWrite,
					rt: sarama.AclResourceTopic,
					p:  "write1.test.com",
				},
				{
					op: sarama.AclOperationRead,
					rt: sarama.AclResourceTopic,
					p:  "read1.test.com",
				},
				{
					op: sarama.AclOperationRead,
					rt: sarama.AclResourceGroup,
					p:  "read1.test.com",
				},
			},
			deleteFilters: nil,
		})
	})
	It("is an existing topic", func() {
		// has no acls
		test(args{
			orig: TopicConfig{
				ACLs: ACLs{
					Writes: bwutil.NewSetFromSlice([]string{}),
					Reads:  bwutil.NewSetFromSlice([]string{}),
				},
			},
			target: TopicConfig{
				Name:              "my_topic",
				Partitions:        3,
				ReplicationFactor: 1,
				Config: TopicConfigDetails{
					RetentionMS: "10",
				},
				ACLs: ACLs{
					Reads:  bwutil.NewSetFromSlice([]string{"read1.test.com"}),
					Writes: bwutil.NewSetFromSlice([]string{"write1.test.com"}),
				},
			},
		}, res{
			acls: []aclType{
				{
					op: sarama.AclOperationWrite,
					rt: sarama.AclResourceTopic,
					p:  "write1.test.com",
				},
				{
					op: sarama.AclOperationRead,
					rt: sarama.AclResourceTopic,
					p:  "read1.test.com",
				},
				{
					op: sarama.AclOperationRead,
					rt: sarama.AclResourceGroup,
					p:  "read1.test.com",
				},
			},
			deleteFilters: nil,
		})
		// swaps write1 for write2 acl, keeps read1
		test(args{
			orig: TopicConfig{
				ACLs: ACLs{
					Reads:  bwutil.NewSetFromSlice([]string{"read1.test.com"}),
					Writes: bwutil.NewSetFromSlice([]string{"write1.test.com"}),
				},
			},
			target: TopicConfig{
				Name:              "my_topic",
				Partitions:        3,
				ReplicationFactor: 1,
				Config: TopicConfigDetails{
					RetentionMS: "10",
				},
				ACLs: ACLs{
					Reads:  bwutil.NewSetFromSlice([]string{"read1.test.com"}),
					Writes: bwutil.NewSetFromSlice([]string{"write2.test.com"}),
				},
			},
		}, res{
			acls: []aclType{
				{
					op: sarama.AclOperationWrite,
					rt: sarama.AclResourceTopic,
					p:  "write2.test.com",
				},
			},
			deleteFilters: []deleteFilter{
				{
					op: sarama.AclOperationWrite,
					p:  "write1.test.com",
				},
			},
		})
	})

})
