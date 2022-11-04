package kafka_dao

import (
	"github.com/Shopify/sarama"
	"github.com/bradfordwagner/go-util"
)

func (d *daoImpl) GetTopicACLs(topic string) (acls ACLs, err error) {
	// setup connection
	err = d.buildAdminConnection()
	if err != nil {
		return
	}

	readACLs, err := getTopicACLS(d.admin.Get(), topic, sarama.AclOperationRead)
	if err != nil {
		return
	}

	writeACLs, err := getTopicACLS(d.admin.Get(), topic, sarama.AclOperationWrite)
	if err != nil {
		return
	}

	// setup the results
	acls.Enabled = !readACLs.IsEmpty() || !writeACLs.IsEmpty() // if either acl is present then acls are enabled
	acls.Reads, acls.Writes = readACLs, writeACLs

	return
}

func getTopicACLS(admin sarama.ClusterAdmin, topic string, operation sarama.AclOperation) (set *bwutil.Set[string], err error) {
	aclFilter := sarama.AclFilter{ResourceName: &topic, Operation: operation, ResourceType: sarama.AclResourceTopic, ResourcePatternTypeFilter: sarama.AclPatternLiteral, PermissionType: sarama.AclPermissionAllow}
	acls, err := admin.ListAcls(aclFilter)
	if err != nil {
		return
	}

	set = bwutil.NewSet[string]()
	for _, acl := range acls {
		for _, principalACL := range acl.Acls {
			set.Add(principalACL.Principal)
		}
	}
	return
}
