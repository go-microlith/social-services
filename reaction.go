package social

import (
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

type ReactionType string

func (reactionType ReactionType) String() string {
	return string(reactionType)
}

const (
	ReactionTypeLike  ReactionType = "Like"
	ReactionTypeLove  ReactionType = "Love"
	ReactionTypeHaha  ReactionType = "Haha"
	ReactionTypeWow   ReactionType = "Wow"
	ReactionTypeSad   ReactionType = "Sad"
	ReactionTypeAngry ReactionType = "Angry"
)

var reactionTypes = []ReactionType{ReactionTypeLike, ReactionTypeLove, ReactionTypeHaha, ReactionTypeWow, ReactionTypeSad, ReactionTypeAngry}

func ParseReaction(reaction string) (ReactionType, error) {
	for _, reactionType := range reactionTypes {
		if strings.ToLower(reactionType.String()) == strings.ToLower(reaction) {
			return reactionType, nil
		}
	}

	return ReactionType(""), fmt.Errorf("%q is not a valid reaction", reaction)
}

type Reaction struct {
	To        uuid.UUID
	Type      ReactionType
	CreatedAt time.Time
}
