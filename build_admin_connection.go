package kafka_dao

import "github.com/Shopify/sarama"

func (d *daoImpl) buildAdminConnection() (err error) {
	if d.admin.Get() == nil {
		err = d.admin.SetF(func() (sarama.ClusterAdmin, error) {
			return d.config.adminBuilder(d.config.brokers, d.config.version)
		})
	}
	return
}
