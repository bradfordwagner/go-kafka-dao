package kafka_dao

import (
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
		}
		var test = func(a args) {
			isBreaking := checkBreakingChange(a.orig, a.target)
			Expect(isBreaking).To(Equal(a.isBreaking))
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
