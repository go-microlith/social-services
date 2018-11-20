package endpoints

import (
	"context"
	"net/http"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
	social "gopkg.in/go-microlith/social-services.v0"
	"github.com/google/uuid"

	"gopkg.in/microlith.v0/sam/tld/rest"
	"gopkg.in/microlith.v0/sam/tld/stor"
)

type reactionsTo struct {
	reactions *stor.Table
}

func ReactionsTo(reactions *stor.Table) rest.Handler {
	return &reactionsTo{
		reactions: reactions,
	}
}

func (endpoint *reactionsTo) ServeREST(ctx context.Context, req events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	to, err := uuid.Parse(req.QueryStringParameters["to"])
	if err != nil {
		return rest.Respond(http.StatusBadRequest, err)
	}

	keyCondition := expression.Key("To").Equal(expression.Value(to.String()))

	reactions := new(social.Reactions)
	scanner, err := endpoint.reactions.Query(ctx, func(query *stor.QueryBuilder) {
		query.KeyCondition(keyCondition)
	})
	for {
		if err != nil {
			rest.Respond(http.StatusInternalServerError, err)
		}

		for scanner.Next() {
			reaction := new(social.Reaction)
			if err = scanner.Scan(reaction); err != nil {
				rest.Respond(http.StatusInternalServerError, err)
			}

			reactions.Total++
			switch reaction.Type {
			case social.ReactionTypeLike:
				reactions.Likes++
			case social.ReactionTypeLove:
				reactions.Loves++
			case social.ReactionTypeHaha:
				reactions.Hahas++
			case social.ReactionTypeWow:
				reactions.Wows++
			case social.ReactionTypeSad:
				reactions.Sads++
			case social.ReactionTypeAngry:
				reactions.Angrys++
			}
		}

		if scanner.LastEvaluatedKey() == nil {
			break
		}
		scanner, err = endpoint.reactions.Query(ctx, func(query *stor.QueryBuilder) {
			query.KeyCondition(keyCondition)
			query.ExclusiveStartKey(scanner.LastEvaluatedKey())
		})
	}

	return rest.Respond(http.StatusOK, reactions)
}
