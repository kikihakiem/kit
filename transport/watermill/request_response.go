package watermill

import (
	"context"

	"github.com/ThreeDotsLabs/watermill/message"
)

// RequestFunc may take information from a publisher request and put it into a
// request context. In Subscribers, RequestFuncs are executed prior to invoking
// the endpoint.
type RequestFunc func(context.Context, *message.Message) context.Context

// SubscriberResponseFunc may take information from a request context and use it to
// manipulate a Publisher. SubscriberResponseFuncs are only executed in
// Subscribers, after invoking the endpoint but prior to publishing a reply.
type SubscriberResponseFunc func(context.Context,
	*message.Message,
	[]*message.Message,
) context.Context

// PublisherResponseFunc may take information from a request and make the
// response available for consumption. PublisherResponseFunc are only executed
// in publishers, after a request has been made, but prior to it being decoded.
type PublisherResponseFunc func(context.Context, *message.Message) context.Context
