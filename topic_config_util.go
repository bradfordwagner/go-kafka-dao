package kafka_dao

import (
	"errors"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/bradfordwagner/go-util"
	"strings"
)

func (t TopicConfig) checkBreakingChange(target TopicConfig) (isBreaking bool, err error) {
	var errs []string
	if t.Name != target.Name {
		errs = append(errs, fmt.Sprintf("name %s!=%s", target.Name, t.Name))
	}
	if t.ReplicationFactor != target.ReplicationFactor {
		errs = append(errs, fmt.Sprintf("replication_factor %d!=%d", target.ReplicationFactor, t.ReplicationFactor))
	}
	if t.Partitions != target.Partitions {
		errs = append(errs, fmt.Sprintf("partitions %d!=%d", target.Partitions, t.Partitions))
	}

	isBreaking = len(errs) > 0
	if isBreaking {
		err = errors.New(fmt.Sprintf("topic=%s contains breaking changes (target!=orig): %s", t.Name, strings.Join(errs, ",")))
	}
	return
}

func (t TopicConfig) ConvertToSaramaTopicDetails() *sarama.TopicDetail {
	configEntries := t.Config.ToConfigMap()
	return &sarama.TopicDetail{
		NumPartitions:     int32(t.Partitions),
		ReplicationFactor: int16(t.ReplicationFactor),
		ReplicaAssignment: nil,
		ConfigEntries:     configEntries,
	}
}

func (tcd TopicConfigDetails) ToConfigMap() map[string]*string {
	configEntries := make(map[string]*string)
	if tcd.RetentionMS != "" {
		configEntries["retention.ms"] = bwutil.Pointer(tcd.RetentionMS)
	}
	return configEntries
}

type aclType int

const (
	aclTypeRead aclType = iota
	aclTypeWrite
)

func convertToSaramaResourceACLs(topic string, set *bwutil.Set[string], t aclType) (res []*sarama.ResourceAcls) {
	if set.IsEmpty() {
		return
	}

	op := sarama.AclOperationWrite
	resourceTypes := []sarama.AclResourceType{sarama.AclResourceTopic}
	if aclTypeRead == t {
		op = sarama.AclOperationRead
		// set the extra resource type for reads - this will create the acl for resource group
		resourceTypes = append(resourceTypes, sarama.AclResourceGroup)
	}

	// create base acl op
	var acls []*sarama.Acl
	for _, k := range set.Keyset() {
		acl := &sarama.Acl{
			Principal:      k,
			Host:           "*",
			Operation:      op,
			PermissionType: sarama.AclPermissionAllow,
		}
		acls = append(acls, acl)
	}

	// create resource types
	// for reads we need acl resource groups set - for consumer groups
	// for reads/writes need to grant access to topic resource for op type
	for _, resourceType := range resourceTypes {
		rn := topic
		if resourceType == sarama.AclResourceGroup {
			rn = "*"
		}

		r := sarama.Resource{
			ResourceType:        resourceType,
			ResourceName:        rn,
			ResourcePatternType: sarama.AclPatternLiteral,
		}

		res = append(res, &sarama.ResourceAcls{
			Resource: r,
			Acls:     acls,
		})
	}

	return
}
