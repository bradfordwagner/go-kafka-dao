package kafka_dao

func (d *daoImpl) GetTopicConfig(topic string) (ok bool, tc TopicConfig, err error) {
	// setup connection
	err = d.buildAdminConnection()
	if err != nil {
		return
	}

	//topics, err := d.admin.Get().ListTopics()
	//if err != nil {
	//	return
	//}

	//details, ok := topics[topic]
	//if !ok {
	//	return
	//}

	return
}
