package kafka_dao

import (
	"errors"
	"github.com/Shopify/sarama"
	"github.com/bradfordwagner/go-kafka-dao/mocks/pkg/mock_sarama"
	"github.com/bradfordwagner/go-util"
	"github.com/golang/mock/gomock"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("UpsertTopic", func() {
	type args struct {
		tc TopicConfig
	}
	type resCreateTopic struct {
		err error
	}
	type resAlterConfig struct {
		err error
	}
	type res struct {
		getTopic      *mockGetTopicConfig
		createTopic   resCreateTopic
		alterConfig   resAlterConfig
		reconcileACLs *mockReconcileACLs
		err           error
	}
	var test = func(a args, r res) {
		ctrl := gomock.NewController(GinkgoT())
		defer ctrl.Finish()

		admin := mock_sarama.NewMockClusterAdmin(ctrl)
		dao := New("broker").(*daoImpl)
		dao.admin = bwutil.NewLockableWithValue[sarama.ClusterAdmin](admin)
		dao.getTopicConfigComponent = r.getTopic
		dao.reconcileACLsComponent = r.reconcileACLs

		f := func() {
			if r.getTopic.err != nil {
				return
			}

			// handle the topic creation/alteration process
			isBreaking, _ := r.getTopic.topicConfig.checkBreakingChange(a.tc)
			if r.getTopic.ok && isBreaking {
				return
			} else if !r.getTopic.ok {
				admin.EXPECT().CreateTopic(a.tc.Name, gomock.Any(), false).Return(r.createTopic.err)
			} else {
				admin.EXPECT().AlterConfig(sarama.TopicResource, a.tc.Name, gomock.Any(), false).Return(r.alterConfig.err)
			}
		}
		f()

		res := dao.UpsertTopic(a.tc)
		if r.err != nil {
			Expect(res).To(Equal(r.err))
		} else {
			Expect(res).ShouldNot(HaveOccurred())
		}
	}

	It("errors getting the topic configuration", func() {
		err := errors.New("expected")
		test(args{
			tc: TopicConfig{},
		}, res{
			getTopic:      newMockGetTopicConfig(false, TopicConfig{}, err),
			createTopic:   resCreateTopic{},
			alterConfig:   resAlterConfig{},
			err:           err,
			reconcileACLs: nil,
		})
	})
	It("has breaking changes (partition 3->4)", func() {
		test(args{
			tc: TopicConfig{
				Name:              "test_topic",
				Partitions:        4,
				ReplicationFactor: 1,
				Config:            TopicConfigDetails{},
				ACLs:              ACLs{},
			},
		}, res{
			getTopic: newMockGetTopicConfig(true, TopicConfig{
				Name:              "test_topic",
				Partitions:        3,
				ReplicationFactor: 1,
				Config:            TopicConfigDetails{},
				ACLs:              ACLs{},
			}, nil),
			createTopic:   resCreateTopic{},
			alterConfig:   resAlterConfig{},
			err:           errors.New("topic=test_topic contains breaking changes (target!=orig): partitions 4!=3"),
			reconcileACLs: nil,
		})
	})
	It("it is a new topic and fails to create", func() {
		err := errors.New("expected")
		test(args{
			tc: TopicConfig{
				Name:              "test_topic",
				Partitions:        4,
				ReplicationFactor: 1,
				Config:            TopicConfigDetails{},
				ACLs:              ACLs{},
			},
		}, res{
			getTopic: newMockGetTopicConfig(false, TopicConfig{}, nil),
			createTopic: resCreateTopic{
				err: err,
			},
			alterConfig:   resAlterConfig{},
			err:           err,
			reconcileACLs: nil,
		})
	})
	It("fails to alter topic configuration", func() {
		err := errors.New("expected")
		test(args{
			tc: TopicConfig{
				Name:              "test_topic",
				Partitions:        4,
				ReplicationFactor: 1,
				Config:            TopicConfigDetails{},
				ACLs:              ACLs{},
			},
		}, res{
			getTopic: newMockGetTopicConfig(true, TopicConfig{
				Name:              "test_topic",
				Partitions:        4,
				ReplicationFactor: 1,
				Config:            TopicConfigDetails{},
				ACLs:              ACLs{},
			}, nil),
			createTopic: resCreateTopic{},
			alterConfig: resAlterConfig{
				err: err,
			},
			err:           err,
			reconcileACLs: nil,
		})
	})
	It("fails to reconcile acls", func() {
		err := errors.New("expected")
		test(args{
			tc: TopicConfig{
				Name:              "test_topic",
				Partitions:        4,
				ReplicationFactor: 1,
				Config:            TopicConfigDetails{},
				ACLs:              ACLs{},
			},
		}, res{
			getTopic:      newMockGetTopicConfig(false, TopicConfig{}, nil),
			createTopic:   resCreateTopic{},
			alterConfig:   resAlterConfig{},
			err:           err,
			reconcileACLs: newMockReconcileACLs(err),
		})
	})
	It("is a smashing success", func() {
		test(args{
			tc: TopicConfig{
				Name:              "test_topic",
				Partitions:        4,
				ReplicationFactor: 1,
				Config:            TopicConfigDetails{},
				ACLs:              ACLs{},
			},
		}, res{
			getTopic:      newMockGetTopicConfig(false, TopicConfig{}, nil),
			createTopic:   resCreateTopic{},
			alterConfig:   resAlterConfig{},
			err:           nil,
			reconcileACLs: newMockReconcileACLs(nil),
		})
	})

})
