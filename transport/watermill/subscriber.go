package watermill

import (
	"context"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/go-kit/kit/endpoint"
	"github.com/go-kit/kit/log"
)

// Subscriber wraps an endpoint and provides a handler for Watermill messages.
type Subscriber struct {
	e            endpoint.Endpoint
	dec          DecodeRequestFunc
	enc          EncodeResponseFunc
	before       []RequestFunc
	after        []SubscriberResponseFunc
	errorEncoder ErrorEncoder
	logger       log.Logger
	middleware   []message.HandlerMiddleware
}

// NewSubscriber constructs a new Subscriber, which provides a handler
// for Watermill messages.
func NewSubscriber(
	e endpoint.Endpoint,
	dec DecodeRequestFunc,
	enc EncodeResponseFunc,
	options ...SubscriberOption,
) *Subscriber {
	s := &Subscriber{
		e:            e,
		dec:          dec,
		enc:          enc,
		errorEncoder: DefaultErrorEncoder,
		logger:       log.NewNopLogger(),
	}
	for _, option := range options {
		option(s)
	}
	return s
}

// SubscriberOption sets an optional parameter for Subscribers.
type SubscriberOption func(*Subscriber)

// SubscriberBefore functions are executed on the publisher delivery object
// before the request is decoded.
func SubscriberBefore(before ...RequestFunc) SubscriberOption {
	return func(c *Subscriber) { c.before = append(c.before, before...) }
}

// SubscriberAfter functions are executed on the Subscriber reply after the
// endpoint is invoked, but before anything is published to the reply.
func SubscriberAfter(after ...SubscriberResponseFunc) SubscriberOption {
	return func(c *Subscriber) { c.after = append(c.after, after...) }
}

// SubscriberErrorEncoder is used to encode errors to the Subscriber reply
// whenever they're encountered in the processing of a request. Clients can
// use this to provide custom error formatting. By default,
// errors will be published with the DefaultErrorEncoder.
func SubscriberErrorEncoder(ee ErrorEncoder) SubscriberOption {
	return func(c *Subscriber) { c.errorEncoder = ee }
}

// SubscriberErrorLogger is used to log non-terminal errors. By default, no errors
// are logged. This is intended as a diagnostic measure. Finer-grained control
// of error handling, including logging in more detail, should be performed in a
// custom SubscriberErrorEncoder which has access to the context.
func SubscriberErrorLogger(logger log.Logger) SubscriberOption {
	return func(c *Subscriber) { c.logger = logger }
}

// SubscriberMiddleware sets handler's middleware(s).
func SubscriberMiddleware(middlewares ...message.HandlerMiddleware) SubscriberOption {
	return func(c *Subscriber) { c.middleware = append(c.middleware, middlewares...) }
}

// ConsumeAndPublish handles Watermill messages and publishes the result.
func (c Subscriber) ConsumeAndPublish(msg *message.Message) (msgs []*message.Message, err error) {
	ctx, cancel := context.WithCancel(msg.Context())
	defer cancel()

	for _, f := range c.before {
		ctx = f(ctx, msg)
	}

	request, err := c.dec(ctx, msg)
	if err != nil {
		c.logger.Log("err", err)
		c.errorEncoder(ctx, err, msgs)
		return
	}

	response, err := c.e(ctx, request)
	if err != nil {
		c.logger.Log("err", err)
		c.errorEncoder(ctx, err, msgs)
		return
	}

	for _, f := range c.after {
		ctx = f(ctx, msg, msgs)
	}

	msgs, err = c.enc(ctx, response)
	if err != nil {
		c.logger.Log("err", err)
		c.errorEncoder(ctx, err, msgs)
		return
	}

	return
}

// ErrorEncoder is responsible for encoding an error to the Subscriber reply.
// Users are encouraged to use custom ErrorEncoders to encode errors to
// their replies, and will likely want to pass and check for their own error
// types.
type ErrorEncoder func(ctx context.Context, err error, msgs []*message.Message)

// DefaultErrorEncoder simply ignores the message.
func DefaultErrorEncoder(ctx context.Context, err error, msgs []*message.Message) {
}
