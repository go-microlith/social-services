package comments

import (
	"context"
	"net/http"

	"github.com/aws/aws-lambda-go/events"
	"github.com/google/uuid"

	"gopkg.in/microlith.v0/sam/tld/rest"

	social "gopkg.in/go-microlith/social-services.v0"
)

type commentOn struct {
	comments  *Service
	parameter string
}

func CommentOn(comments *Service, parameter string) rest.Handler {
	return &commentOn{
		comments:  comments,
		parameter: parameter,
	}
}

func (endpoint *commentOn) ServeREST(ctx context.Context, req events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	param := req.PathParameters[endpoint.parameter]
	on, err := uuid.Parse(param)
	if err != nil {
		return rest.Respond(http.StatusBadRequest, err)
	}

	comment := new(social.Comment)
	if err := rest.RequestBody(req, comment); err != nil {
		return rest.Respond(http.StatusBadRequest, err)
	}

	comment, err = endpoint.comments.CreateComment(ctx, on, comment.Body)
	if err != nil {
		return rest.Respond(http.StatusInternalServerError, err)
	}

	return rest.Respond(http.StatusCreated, comment)
}
