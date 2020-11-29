package watermill

import (
	"context"

	"github.com/ThreeDotsLabs/watermill/message"
)

// DecodeRequestFunc extracts a user-domain request object from a Watermill Message object.
// It is designed to be used in Watermill Subscribers.
type DecodeRequestFunc func(context.Context, *message.Message) (request interface{}, err error)

// EncodeRequestFunc encodes the passed request object into a Watermill Message object.
// It is designed to be used in Watermill Publishers.
type EncodeRequestFunc func(context.Context, interface{}) (request *message.Message, err error)

// EncodeResponseFunc encodes the passed reponse object to a Watermill Message object.
// It is designed to be used in Watermill Subscribers.
type EncodeResponseFunc func(context.Context, interface{}) (response []*message.Message, err error)

// DecodeResponseFunc extracts a user-domain response object from a Watermill Message object.
// It is designed to be used in Watermill Publishers.
type DecodeResponseFunc func(context.Context, *message.Message) (response interface{}, err error)
