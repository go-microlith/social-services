package processors

import (
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
	social "gopkg.in/go-microlith/social-services.v0"
	"github.com/google/uuid"

	"gopkg.in/microlith.v0/sam/tld/stor"
)

type purgeReactions struct {
	reactions *stor.Table
}

func PurgeReactions(reactions *stor.Table) social.Deleter {
	return &purgeReactions{
		reactions: reactions,
	}
}

func (deleter *purgeReactions) Source() social.Querier {
	return deleter.reactions
}

func (deleter *purgeReactions) Table() *stor.Table {
	return deleter.reactions
}

func (*purgeReactions) Query(id uuid.UUID, query *stor.QueryBuilder) {
	query.KeyCondition(expression.Key("To").Equal(expression.Value(id.String())))
	query.Project(expression.NamesList(expression.Name("To"), expression.Name("CreatedAt")))
}
