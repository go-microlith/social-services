package social

import (
	"context"
	"crypto/md5"
	"fmt"

	"github.com/aws/aws-lambda-go/events"
	"github.com/google/uuid"

	"gopkg.in/microlith.v0/sam/tld/stor"
	"gopkg.in/microlith.v0/sam/tld/strm"
)

type watchForDeletes struct {
	objectDeleted *strm.Stream
	id            func(events.DynamoDBStreamRecord) string
}

func WatchForDeletes(objectDeleted *strm.Stream, id func(events.DynamoDBStreamRecord) string) stor.Watcher {
	return &watchForDeletes{
		objectDeleted: objectDeleted,
		id:            id,
	}
}

func (watcher *watchForDeletes) Watch(ctx context.Context, evt events.DynamoDBEvent) error {
	defer func() {
		if err := watcher.objectDeleted.Flush(ctx); err != nil {
			panic(err)
		}
	}()

	for _, record := range evt.Records {
		if record.EventName == string(events.DynamoDBOperationTypeRemove) {
			id, err := uuid.Parse(watcher.id(record.Change))
			if err != nil {
				return err
			}

			partitionKey := fmt.Sprintf("%X", md5.New().Sum([]byte(id.String())))
			delete := &Delete{ID: id}
			if err := watcher.objectDeleted.Publish(ctx, partitionKey, delete); err != nil {
				return err
			}
		}
	}

	return nil
}
