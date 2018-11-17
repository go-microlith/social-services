package social

import (
	"context"

	"github.com/aws/aws-lambda-go/events"
	"github.com/google/uuid"
	"gopkg.in/microlith.v0/sam/tld/stor"
	"gopkg.in/microlith.v0/sam/tld/strm"
)

type Querier interface {
	Query(context.Context, ...stor.QueryBuilderFunc) (*stor.Result, error)
}

type Deleter interface {
	Source() Querier
	Table() *stor.Table
	Query(id uuid.UUID, query *stor.QueryBuilder)
	Key() interface{}
}

type Delete struct {
	ID uuid.UUID
}

type objectDeleted struct {
	deleter Deleter
}

func ObjectDeleted(deleter Deleter) strm.Processor {
	return &objectDeleted{
		deleter: deleter,
	}
}

func (processor *objectDeleted) Process(ctx context.Context, evt events.KinesisEvent) error {
	keys := []interface{}{}

	scanner := strm.NewScanner(evt)
	for scanner.Next() {
		partitionKey := ""
		delete := new(Delete)
		if err := scanner.Scan(&partitionKey, delete); err != nil {
			return err
		}

		result, err := processor.deleter.Source().Query(ctx, func(query *stor.QueryBuilder) {
			processor.deleter.Query(delete.ID, query)
		})
		for {
			if err != nil {
				return err
			}

			for result.Next() {
				key := processor.deleter.Key()
				if err := result.Scan(key); err != nil {
					return err
				}
				keys = append(keys, key)
			}

			if result.LastEvaluatedKey() == nil {
				break
			}
			result, err = processor.deleter.Source().Query(ctx, func(query *stor.QueryBuilder) {
				query.ExclusiveStartKey(result.LastEvaluatedKey())
				processor.deleter.Query(delete.ID, query)
			})
		}
	}

	for len(keys) > 0 {
		i := 0

		result, err := stor.WriteBatch(ctx, func(batch *stor.BatchWriteBuilder) {
			for i = 0; i < len(keys) && i < 100; i++ {
				batch.Delete(processor.deleter.Table(), keys[i])
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

		keys = keys[i:]
	}

	return nil
}
