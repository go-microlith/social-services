package processors

import (
	"gopkg.in/microlith.v0/sam/tld/stor"

	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
	social "github.com/go-microlith/social-services"
	"github.com/google/uuid"
)

type purgeComments struct {
	comments *stor.Table
	on       *stor.Index
}

func PurgeComments(comments *stor.Table, on *stor.Index) social.Deleter {
	return &purgeComments{
		comments: comments,
		on:       on,
	}
}

func (deleter *purgeComments) Source() social.Querier {
	return deleter.on
}

func (deleter *purgeComments) Table() *stor.Table {
	return deleter.comments
}

func (*purgeComments) Query(id uuid.UUID, query *stor.QueryBuilder) {
	query.KeyCondition(expression.Key("On").Equal(expression.Value(id.String())))
	query.Project(expression.NamesList(expression.Name("ID")))
}

func (*purgeComments) Key() interface{} {
	return new(social.Comment)
}
