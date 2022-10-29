package kafka_dao

//go:generate mockgen -destination/mocks/pkg/mock_sarama/module.go -package mock_sarama github.com/Shopify/sarama ClusterAdmin,Client
