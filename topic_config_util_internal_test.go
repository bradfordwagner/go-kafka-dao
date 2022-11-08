package kafka_dao

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("TopicConfigUtil", func() {
	It("test", func() { Expect(true).To(BeTrue()) })

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
