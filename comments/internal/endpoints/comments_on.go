package endpoints

import (
	"context"
	"net/http"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
	"github.com/google/uuid"

	"gopkg.in/microlith.v0/sam/tld/rest"
	"gopkg.in/microlith.v0/sam/tld/stor"

	social "github.com/go-microlith/social-services"
)

type commentsOn struct {
	on *stor.Index
}

func CommentsOn(on *stor.Index) rest.Handler {
	return &commentsOn{
		on: on,
	}
}

func (endpoint *commentsOn) ServeREST(ctx context.Context, req events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	on, err := uuid.Parse(req.QueryStringParameters["on"])
	if err != nil {
		return rest.Respond(http.StatusBadRequest, err)
	}

	keyCondition := expression.Key("On").Equal(expression.Value(on.String()))

	comments := []*social.Comment{}
	scanner, err := endpoint.on.Query(ctx, func(query *stor.QueryBuilder) {
		query.KeyCondition(keyCondition)
	})
	for {
		if err != nil {
			return rest.Respond(http.StatusInternalServerError, err)
		}

		for scanner.Next() {
			comment := &social.Comment{}
			if err = scanner.Scan(comment); err != nil {
				return rest.Respond(http.StatusInternalServerError, err)
			}
			comments = append(comments, comment)
		}

		if scanner.LastEvaluatedKey() == nil {
			break
		}
		scanner, err = endpoint.on.Query(ctx, func(query *stor.QueryBuilder) {
			query.KeyCondition(keyCondition)
			query.ExclusiveStartKey(scanner.LastEvaluatedKey())
		})
	}

	return rest.Respond(http.StatusOK, comments)
}
