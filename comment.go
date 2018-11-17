package social

import (
	"time"

	"github.com/google/uuid"
)

type Comment struct {
	ID        uuid.UUID
	On        uuid.UUID
	Body      string
	CreatedAt time.Time
}
