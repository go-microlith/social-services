package endpoints

import (
	"context"
	"net/http"
	"time"

	"github.com/aws/aws-lambda-go/events"
	social "github.com/go-microlith/social-services"

	"gopkg.in/microlith.v0/sam/tld/rest"
	"gopkg.in/microlith.v0/sam/tld/stor"
)

type reactTo struct {
	reactions *stor.Table
}

func ReactTo(reactions *stor.Table) rest.Handler {
	return &reactTo{
		reactions: reactions,
	}
}

func (endpoint *reactTo) ServeREST(ctx context.Context, req events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	reaction := new(social.Reaction)
	if err := rest.RequestBody(req, reaction); err != nil {
		return rest.Respond(http.StatusBadRequest, err)
	}
	reaction.CreatedAt = time.Now()

	if err := endpoint.reactions.Put(ctx, *reaction); err != nil {
		return rest.Respond(http.StatusInternalServerError, err)
	}

	return rest.Respond(http.StatusCreated, reaction)
}
