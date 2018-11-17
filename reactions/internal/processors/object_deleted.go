package processors

import (
	"context"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
	social "github.com/go-microlith/social-services"

	"gopkg.in/microlith.v0/sam/tld/stor"
	"gopkg.in/microlith.v0/sam/tld/strm"
)

type objectDeleted struct {
	reactions *stor.Table
}

func ObjectDeleted(reactions *stor.Table) strm.Processor {
	return &objectDeleted{
		reactions: reactions,
	}
}

func (processor *objectDeleted) Process(ctx context.Context, evt events.KinesisEvent) error {
	reactions := []*social.Reaction{}

	scanner := strm.NewScanner(evt)
	for scanner.Next() {
		partitionKey := ""
		delete := new(social.ObjectDeleted)
		if err := scanner.Scan(&partitionKey, delete); err != nil {
			return err
		}

		result, err := processor.reactions.Query(ctx, func(query *stor.QueryBuilder) {
			query.KeyCondition(expression.Key("To").Equal(expression.Value(delete.ID.String())))
			query.Project(expression.NamesList(expression.Name("To"), expression.Name("CreatedAt")))
		})
		for {
			if err != nil {
				return err
			}

			for result.Next() {
				reaction := new(social.Reaction)
				if err := result.Scan(reaction); err != nil {
					return err
				}
				reactions = append(reactions, reaction)
			}

			if result.LastEvaluatedKey() == nil {
				break
			}
			result, err = processor.reactions.Query(ctx, func(query *stor.QueryBuilder) {
				query.KeyCondition(expression.Key("To").Equal(expression.Value(delete.ID.String())))
				query.Project(expression.NamesList(expression.Name("To"), expression.Name("CreatedAt")))
				query.ExclusiveStartKey(result.LastEvaluatedKey())
			})
		}
	}

	for len(reactions) > 0 {
		i := 0

		result, err := stor.WriteBatch(ctx, func(batch *stor.BatchWriteBuilder) {
			for i = 0; i < len(reactions) && i < 100; i++ {
				batch.Delete(processor.reactions, *reactions[i])
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

		reactions = reactions[i:]
	}

	return nil
}
