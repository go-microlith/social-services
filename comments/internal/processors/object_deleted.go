package processors

import (
	"context"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"

	"gopkg.in/microlith.v0/sam/tld/stor"
	"gopkg.in/microlith.v0/sam/tld/strm"

	social "github.com/go-microlith/social-services"
)

type objectDeleted struct {
	comments *stor.Table
	on       *stor.Index
}

func ObjectDeleted(comments *stor.Table, on *stor.Index) strm.Processor {
	return &objectDeleted{
		comments: comments,
		on:       on,
	}
}

func (processor *objectDeleted) Process(ctx context.Context, evt events.KinesisEvent) error {
	comments := []*social.Comment{}

	scanner := strm.NewScanner(evt)
	for scanner.Next() {
		partitionKey := ""
		delete := new(social.ObjectDeleted)
		if err := scanner.Scan(&partitionKey, delete); err != nil {
			return err
		}

		result, err := processor.on.Query(ctx, func(query *stor.QueryBuilder) {
			query.KeyCondition(expression.Key("On").Equal(expression.Value(delete.ID.String())))
			query.Project(expression.NamesList(expression.Name("ID")))
		})
		for {
			if err != nil {
				return err
			}

			for result.Next() {
				comment := new(social.Comment)
				if err := result.Scan(comment); err != nil {
					return err
				}
				comments = append(comments, comment)
			}

			if result.LastEvaluatedKey() == nil {
				break
			}
			result, err = processor.on.Query(ctx, func(query *stor.QueryBuilder) {
				query.KeyCondition(expression.Key("On").Equal(expression.Value(delete.ID.String())))
				query.Project(expression.NamesList(expression.Name("ID")))
				query.ExclusiveStartKey(result.LastEvaluatedKey())
			})
		}
	}

	for len(comments) > 0 {
		i := 0

		result, err := stor.WriteBatch(ctx, func(batch *stor.BatchWriteBuilder) {
			for i = 0; i < len(comments) && i < 100; i++ {
				batch.Delete(processor.comments, *comments[i])
			}
		})
		for {
			if err != nil {
				return err
			}

			if !result.More() {
				break
			}
			result, err = result.Continue(ctx)
		}

		comments = comments[i:]
	}

	return nil
}
