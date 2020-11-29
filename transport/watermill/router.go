package watermill

import (
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
)

// Router routes messages to its handlers
type Router struct {
	*message.Router

	subscriber message.Subscriber
	publisher  message.Publisher
}

// NewRouter initialize new router
func NewRouter(config message.RouterConfig, logger watermill.LoggerAdapter, subscriber message.Subscriber, publisher message.Publisher) (*Router, error) {
	router, err := message.NewRouter(config, logger)
	return &Router{
		Router:     router,
		subscriber: subscriber,
		publisher:  publisher,
	}, err
}

// NoPublishHandle handle specified topic without publishing the results
func (r *Router) NoPublishHandle(topic string, subscriber *Subscriber) *message.Handler {
	h := r.AddHandler(
		topic,
		topic,
		r.subscriber,
		"",
		nopPublisher{},
		subscriber.ConsumeAndPublish,
	)

	for _, middleware := range subscriber.middleware {
		h.AddMiddleware(middleware)
	}

	return h
}

// Handle handle specified topic and publish the results to publishTopic
func (r *Router) Handle(topic string, publishTopic string, subscriber *Subscriber) *message.Handler {
	h := r.AddHandler(
		topic,
		topic,
		r.subscriber,
		publishTopic,
		r.publisher,
		subscriber.ConsumeAndPublish,
	)

	for _, middleware := range subscriber.middleware {
		h.AddMiddleware(middleware)
	}

	return h
}

// nopPublisher will simply ignore results from handler
// as opposed to publish events.
type nopPublisher struct{}

func (nopPublisher) Publish(topic string, messages ...*message.Message) error {
	return message.ErrOutputInNoPublisherHandler
}

func (nopPublisher) Close() error {
	return nil
}
