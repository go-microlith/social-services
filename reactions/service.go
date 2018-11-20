package reactions

import (
	"context"
	"net/http"

	"github.com/google/uuid"

	"gopkg.in/microlith.v0/sam/tld"
	"gopkg.in/microlith.v0/sam/tld/rest"
	"gopkg.in/microlith.v0/sam/tld/stor"
	"gopkg.in/microlith.v0/sam/tld/strm"

	social "gopkg.in/go-microlith/social-services.v0"
	"gopkg.in/go-microlith/social-services.v0/reactions/internal/endpoints"
	"gopkg.in/go-microlith/social-services.v0/reactions/internal/processors"
)

type Service struct {
	reactionsToEndpoint *rest.Endpoint
	reactToEndpoint     *rest.Endpoint
	objectDeleted       *strm.Stream
}

func New(objectDeleted *strm.Stream) *Service {
	return &Service{
		objectDeleted: objectDeleted,
	}
}

func (service *Service) Build(svc *tld.ServiceBuilder) {
	reactions := svc.Table("reactions", stor.String("To"), stor.String("CreatedAt"), stor.DefaultChangeType)

	svc.Processor("purge-reactions", social.ObjectDeleted(processors.PurgeReactions(reactions)), func(processor *strm.ProcessorBuilder) {
		processor.Process(service.objectDeleted, strm.StartingPositionTrimHorizon)
	})

	svc.API(func(api *rest.APIBuilder) {
		api.Scope("/reactions", func(scope *rest.ScopeBuilder) {
			scope.Response(http.StatusBadRequest)
			scope.Response(http.StatusInternalServerError)

			scope.Get("reactions-to", endpoints.ReactionsTo(reactions), &service.reactionsToEndpoint, func(endpoint *rest.EndpointBuilder) {
				endpoint.Query("to", true)

				endpoint.Response(http.StatusOK)
				endpoint.Response(http.StatusNotFound)
			})

			scope.Post("react-to", endpoints.ReactTo(reactions), &service.reactToEndpoint, func(endpoint *rest.EndpointBuilder) {
				endpoint.Response(http.StatusCreated)
			})
		})
	})
}

func (service *Service) ReactionsTo(ctx context.Context, to uuid.UUID) (*social.Reactions, error) {
	resp, err := service.reactionsToEndpoint.Call(ctx, nil, func(request *rest.RequestBuilder) {
		request.Query("to", to.String())
	})
	if err != nil {
		return nil, err
	}

	reactions := new(social.Reactions)
	switch resp.StatusCode {
	case http.StatusOK:
		return reactions, rest.ResponseBody(resp, reactions)
	default:
		return nil, rest.ErrorResponse(resp, nil)
	}
}

func (service *Service) ReactTo(ctx context.Context, to uuid.UUID, reactionType social.ReactionType) error {
	reaction := &social.Reaction{To: to, Type: reactionType}
	resp, err := service.reactToEndpoint.Call(ctx, reaction)
	if err != nil {
		return err
	}

	switch resp.StatusCode {
	case http.StatusCreated:
		return nil
	default:
		return rest.ErrorResponse(resp, nil)
	}
}
