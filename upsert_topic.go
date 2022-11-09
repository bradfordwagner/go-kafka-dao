package kafka_dao

import (
	"github.com/Shopify/sarama"
)

// UpsertTopic - adds a topic to kafka
func (d *daoImpl) UpsertTopic(t TopicConfig) (err error) {
	//	setup admin conn
	err = d.buildAdminConnection()
	if err != nil {
		return err
	}

	ok, existingConfig, err := d.GetTopicConfig(t.Name)
	if err != nil {
		return
	}

	isBreaking, err := existingConfig.checkBreakingChange(t)
	if ok && isBreaking {
		return
	} else if !ok {
		err = d.admin.Get().CreateTopic(t.Name, t.ConvertToSaramaTopicDetails(), false)
	} else {
		err = d.admin.Get().AlterConfig(sarama.TopicResource, t.Name, t.Config.ToConfigMap(), false)
	}
	if err != nil {
		return
	}

	err = d.reconcileACLsComponent.reconcileACLs(existingConfig, t)
	if err != nil {
		return
	}

	return
}
