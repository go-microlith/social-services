package comments

import (
	"context"
	"net/http"

	"github.com/aws/aws-lambda-go/events"
	"github.com/google/uuid"

	"gopkg.in/microlith.v0/sam/tld"
	"gopkg.in/microlith.v0/sam/tld/rest"
	"gopkg.in/microlith.v0/sam/tld/stor"
	"gopkg.in/microlith.v0/sam/tld/strm"

	social "github.com/go-microlith/social-services"
	"github.com/go-microlith/social-services/comments/internal/endpoints"
	"github.com/go-microlith/social-services/comments/internal/processors"
)

type Service struct {
	commentsOnEndpoint    *rest.Endpoint
	createCommentEndpoint *rest.Endpoint
	objectDeleted         *strm.Stream
}

func New(objectDeleted *strm.Stream) *Service {
	return &Service{
		objectDeleted: objectDeleted,
	}
}

func (service *Service) Build(builder *tld.ServiceBuilder) {
	var on *stor.Index
	comments := builder.Table("comments", stor.String("ID"), nil, stor.ChangeTypeNewAndOld, func(table *stor.TableBuilder) {
		on = table.GlobalIndex("on-object", stor.String("On"), stor.String("CreatedAt"), stor.ProjectionTypeAll)
	})

	watcher := social.WatchForDeletes(service.objectDeleted, func(change events.DynamoDBStreamRecord) string { return change.Keys["ID"].String() })
	builder.Watcher("watch-for-deletes", watcher, func(watcher *stor.WatcherBuilder) {
		watcher.Watch(comments, strm.StartingPositionTrimHorizon)
	})

	builder.Processor("object-deleted", social.ObjectDeleted(processors.PurgeComments(comments, on)), func(processor *strm.ProcessorBuilder) {
		processor.Process(service.objectDeleted, strm.StartingPositionTrimHorizon)
	})

	builder.API(func(api *rest.APIBuilder) {
		api.Scope("/comments", func(scope *rest.ScopeBuilder) {
			scope.Response(http.StatusBadRequest)
			scope.Response(http.StatusInternalServerError)

			scope.Get("comments-on", endpoints.CommentsOn(on), &service.commentsOnEndpoint, func(endpoint *rest.EndpointBuilder) {
				endpoint.Query("on", true)
				endpoint.Response(http.StatusOK)
			})

			scope.Post("create-comment", endpoints.CreateComment(comments), &service.createCommentEndpoint, func(endpoint *rest.EndpointBuilder) {
				endpoint.Response(http.StatusCreated)
			})
		})
	})
}

func (service *Service) CommentsOn(ctx context.Context, on uuid.UUID) ([]*social.Comment, error) {
	resp, err := service.commentsOnEndpoint.Call(ctx, nil, func(request *rest.RequestBuilder) {
		request.Query("on", on.String())
	})
	if err != nil {
		return nil, err
	}

	comments := []*social.Comment{}
	switch resp.StatusCode {
	case http.StatusOK:
		return comments, rest.ResponseBody(resp, &comments)
	default:
		return nil, rest.ErrorResponse(resp, nil)
	}
}

func (service *Service) CreateComment(ctx context.Context, on uuid.UUID, body string) (*social.Comment, error) {
	comment := &social.Comment{On: on, Body: body}
	resp, err := service.createCommentEndpoint.Call(ctx, comment)
	if err != nil {
		return nil, err
	}

	switch resp.StatusCode {
	case http.StatusCreated:
		return comment, rest.ResponseBody(resp, comment)
	default:
		return nil, rest.ErrorResponse(resp, nil)
	}
}
