package kafka_dao

import (
	"errors"
	"github.com/Shopify/sarama"
	"github.com/bradfordwagner/go-util"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("TopicConfigUtil", func() {

	Context("checkBreakingChange", func() {
		type args struct {
			orig, target TopicConfig
			isBreaking   bool
			errs         string
		}
		var test = func(a args) {
			isBreaking, err := a.orig.checkBreakingChange(a.target)
			Expect(isBreaking).To(Equal(a.isBreaking))
			if a.errs == "" {
				Expect(err).ShouldNot(HaveOccurred())
			} else {
				Expect(err).To(Equal(errors.New(a.errs)))
			}
		}
		It("has different replication factor - breaks", func() {
			test(args{
				orig: TopicConfig{
					Name:              "t1",
					Partitions:        3,
					ReplicationFactor: 2,
					Config:            TopicConfigDetails{},
					ACLs:              ACLs{},
				},
				target: TopicConfig{
					Name:              "t1",
					Partitions:        3,
					ReplicationFactor: 1,
					Config:            TopicConfigDetails{},
					ACLs:              ACLs{},
				},
				isBreaking: true,
				errs:       "topic=t1 contains breaking changes (target!=orig): replication_factor 1!=2",
			})
		})
		It("has different num partitions - breaks", func() {
			test(args{
				orig: TopicConfig{
					Name:              "t1",
					Partitions:        3,
					ReplicationFactor: 2,
					Config:            TopicConfigDetails{},
					ACLs:              ACLs{},
				},
				target: TopicConfig{
					Name:              "t1",
					Partitions:        1,
					ReplicationFactor: 2,
					Config:            TopicConfigDetails{},
					ACLs:              ACLs{},
				},
				isBreaking: true,
				errs:       "topic=t1 contains breaking changes (target!=orig): partitions 1!=3",
			})
		})
		It("has different acls - passes", func() {
			test(args{
				orig: TopicConfig{
					Name:              "t1",
					Partitions:        3,
					ReplicationFactor: 2,
					Config:            TopicConfigDetails{},
					ACLs: ACLs{
						Enabled: true,
					},
				},
				target: TopicConfig{
					Name:              "t1",
					Partitions:        3,
					ReplicationFactor: 2,
					Config:            TopicConfigDetails{},
					ACLs: ACLs{
						Enabled: false,
					},
				},
				isBreaking: false,
				errs:       "",
			})
		})
		It("has multiple failures - fails", func() {
			test(args{
				orig: TopicConfig{
					Name:              "t1",
					Partitions:        1,
					ReplicationFactor: 1,
					Config: TopicConfigDetails{
						RetentionMS: "10",
					},
					ACLs: ACLs{},
				},
				target: TopicConfig{
					Name:              "t1",
					Partitions:        2,
					ReplicationFactor: 2,
					Config: TopicConfigDetails{
						RetentionMS: "100",
					},
					ACLs: ACLs{},
				},
				isBreaking: true,
				errs:       "topic=t1 contains breaking changes (target!=orig): replication_factor 2!=1,partitions 2!=1",
			})
		})
		It("has different retention - passes", func() {
			test(args{
				orig: TopicConfig{
					Name:              "t1",
					Partitions:        3,
					ReplicationFactor: 2,
					Config: TopicConfigDetails{
						RetentionMS: "10",
					},
					ACLs: ACLs{},
				},
				target: TopicConfig{
					Name:              "t1",
					Partitions:        3,
					ReplicationFactor: 2,
					Config: TopicConfigDetails{
						RetentionMS: "100",
					},
					ACLs: ACLs{},
				},
				isBreaking: false,
				errs:       "",
			})
		})
	})

	Context("ConvertToSaramaTopicDetails", func() {
		type args struct {
			tc TopicConfig
			td *sarama.TopicDetail
		}
		var test = func(a args) {
			Expect(a.tc.ConvertToSaramaTopicDetails()).To(Equal(a.td))
		}
		It("has retention set", func() {
			test(args{
				tc: TopicConfig{
					Name:              "t1",
					Partitions:        3,
					ReplicationFactor: 2,
					Config: TopicConfigDetails{
						RetentionMS: "1",
					},
					// doesn't care about acls
					ACLs: ACLs{},
				},
				td: &sarama.TopicDetail{
					NumPartitions:     3,
					ReplicationFactor: 2,
					ReplicaAssignment: nil,
					ConfigEntries: map[string]*string{
						"retention.ms": bwutil.Pointer("1"),
					},
				},
			})
		})
		It("has no retention set", func() {
			test(args{
				tc: TopicConfig{
					Name:              "t1",
					Partitions:        3,
					ReplicationFactor: 2,
					Config:            TopicConfigDetails{},
					// doesn't care about acls
					ACLs: ACLs{},
				},
				td: &sarama.TopicDetail{
					NumPartitions:     3,
					ReplicationFactor: 2,
					ReplicaAssignment: nil,
					ConfigEntries:     map[string]*string{},
				},
			})
		})
	})

})
