package reactions

import (
	"context"
	"net/http"

	"github.com/aws/aws-lambda-go/events"
	social "github.com/go-microlith/social-services"
	"github.com/google/uuid"
	"gopkg.in/microlith.v0/sam/tld/rest"
)

type reactTo struct {
	to        string
	reaction  string
	reactions *Service
}

func ReactTo(to, reaction string, reactions *Service) rest.Handler {
	return &reactTo{
		to:        to,
		reaction:  reaction,
		reactions: reactions,
	}
}

func (endpoint *reactTo) ServeREST(ctx context.Context, req events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	to, err := uuid.Parse(req.PathParameters[endpoint.to])
	if err != nil {
		return rest.Respond(http.StatusBadRequest, err)
	}

	reaction, err := social.ParseReaction(req.PathParameters[endpoint.reaction])
	if err != nil {
		return rest.Respond(http.StatusBadRequest, err)
	}

	if err := endpoint.reactions.ReactTo(ctx, to, reaction); err != nil {
		return rest.Respond(http.StatusInternalServerError, err)
	}

	return rest.Respond(http.StatusCreated, nil)
}
