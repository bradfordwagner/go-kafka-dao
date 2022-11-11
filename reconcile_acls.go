package kafka_dao

import "github.com/Shopify/sarama"

func (d *daoImpl) reconcileACLs(orig, target TopicConfig) (err error) {
	// assume admin conn has been built
	deleteReads := convertToSaramaDeleteAclsFilter(target.Name, orig.ACLs.Reads.Difference(target.ACLs.Reads), sarama.AclOperationRead)
	deleteWrites := convertToSaramaDeleteAclsFilter(target.Name, orig.ACLs.Writes.Difference(target.ACLs.Writes), sarama.AclOperationWrite)
	createReads := convertToSaramaResourceACLs(target.Name, target.ACLs.Reads.Difference(orig.ACLs.Reads), aclTypeRead)
	createWrites := convertToSaramaResourceACLs(target.Name, target.ACLs.Writes.Difference(orig.ACLs.Writes), aclTypeWrite)

	create := [][]*sarama.ResourceAcls{createReads, createWrites}
	for _, acls := range create {
		err = d.admin.Get().CreateACLs(acls)
		if err != nil {
			return
		}
	}

	delete := [][]sarama.AclFilter{deleteReads, deleteWrites}
	for _, filters := range delete {
		for _, filter := range filters {
			_, err = d.admin.Get().DeleteACL(filter, false)
			if err != nil {
				return
			}
		}
	}

	return
}
