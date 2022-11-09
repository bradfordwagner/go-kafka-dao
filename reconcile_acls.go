package kafka_dao

import "github.com/Shopify/sarama"

func (d *daoImpl) reconcileACLs(orig, target TopicConfig) (err error) {
	// assume admin conn has been built
	//deleteReads := orig.ACLs.Reads.Difference(target.ACLs.Reads)
	//deleteWrites := orig.ACLs.Writes.Difference(target.ACLs.Writes)
	createReads := convertToSaramaResourceACLs(target.Name, target.ACLs.Reads.Difference(orig.ACLs.Reads), aclTypeRead)
	createWrites := convertToSaramaResourceACLs(target.Name, target.ACLs.Writes.Difference(orig.ACLs.Writes), aclTypeWrite)

	create := [][]*sarama.ResourceAcls{createReads, createWrites}
	for _, acls := range create {
		err = d.admin.Get().CreateACLs(acls)
		if err != nil {
			return
		}
	}

	//oldACLFilter := sarama.AclFilter{ResourceName: &topic.Name, Operation: operation, ResourceType: sarama.AclResourceTopic, ResourcePatternTypeFilter: sarama.AclPatternLiteral, PermissionType: sarama.AclPermissionAllow, Principal: &oldACL}
	//_, err = admin.DeleteACL(oldACLFilter, false)

	return
}
