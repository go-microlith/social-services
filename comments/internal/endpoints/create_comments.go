package endpoints

import (
	"context"
	"net/http"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/google/uuid"

	"gopkg.in/microlith.v0/sam/tld/rest"
	"gopkg.in/microlith.v0/sam/tld/stor"

	social "github.com/go-microlith/social-services"
)

type createComment struct {
	comments *stor.Table
}

func CreateComment(comments *stor.Table) rest.Handler {
	return &createComment{
		comments: comments,
	}
}

func (endpoint *createComment) ServeREST(ctx context.Context, req events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	comment := new(social.Comment)
	if err := rest.RequestBody(req, comment); err != nil {
		return rest.Respond(http.StatusBadRequest, err)
	}
	comment.ID = uuid.New()
	comment.CreatedAt = time.Now()

	if err := endpoint.comments.Put(ctx, *comment); err != nil {
		return rest.Respond(http.StatusInternalServerError, err)
	}

	return rest.Respond(http.StatusCreated, comment)
}
