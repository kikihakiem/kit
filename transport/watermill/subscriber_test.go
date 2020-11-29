package watermill_test

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"testing"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/gochannel"

	watermilltransport "github.com/go-kit/kit/transport/watermill"
)

type testStruct struct {
	A int `json:"a"`
	B int `json:"b"`
}

var (
	nopLogger = watermill.NopLogger{}
)

// TestSubscriberBadDecode checks if decoder errors are handled properly.
func TestSubscriberBadDecode(t *testing.T) {
	publisher, subscriber := createPersistentPubSub(t)
	errChan := make(chan error)
	expectedErr := errors.New("bad decode")
	router, _ := watermilltransport.NewRouter(message.RouterConfig{}, nopLogger, subscriber, publisher)
	router.NoPublishHandle(
		"test",
		watermilltransport.NewSubscriber(
			func(context.Context, interface{}) (interface{}, error) { return struct{}{}, nil }, // bogus endpoint
			func(context.Context, *message.Message) (interface{}, error) { return nil, expectedErr },
			nil, // encode
			watermilltransport.SubscriberErrorEncoder(errorEncoder(errChan)),
		),
	)

	go func() {
		router.Run(context.Background())
	}()

	go publishSimpleMessage(publisher, "test", []byte("message"))
	err := <-errChan
	if err != expectedErr {
		t.Errorf("want %s, have %s", expectedErr, err)
	}
}

// TestSubscriberBadEndpoint checks if endpoint errors are handled properly.
func TestSubscriberBadEndpoint(t *testing.T) {
	publisher, subscriber := createPersistentPubSub(t)
	errChan := make(chan error)
	expectedErr := errors.New("bad endpoint")
	router, _ := watermilltransport.NewRouter(message.RouterConfig{}, nopLogger, subscriber, publisher)
	router.NoPublishHandle(
		"test",
		watermilltransport.NewSubscriber(
			func(context.Context, interface{}) (interface{}, error) { return nil, expectedErr },
			func(context.Context, *message.Message) (interface{}, error) { return struct{}{}, nil }, // bogus decoder
			nil, // encode
			watermilltransport.SubscriberErrorEncoder(errorEncoder(errChan)),
		),
	)

	go func() {
		router.Run(context.Background())
	}()

	go publishSimpleMessage(publisher, "test", []byte("message"))
	err := <-errChan
	if err != expectedErr {
		t.Errorf("want %s, have %s", expectedErr, err)
	}
}

// TestSubscriberBadEncoder checks if encoder errors are handled properly.
func TestSubscriberBadEncode(t *testing.T) {
	publisher, subscriber := createPersistentPubSub(t)
	errChan := make(chan error)
	expectedErr := errors.New("bad encode")
	router, _ := watermilltransport.NewRouter(message.RouterConfig{}, nopLogger, subscriber, publisher)
	router.NoPublishHandle(
		"test",
		watermilltransport.NewSubscriber(
			func(context.Context, interface{}) (interface{}, error) { return struct{}{}, nil },      // bogus endpoint
			func(context.Context, *message.Message) (interface{}, error) { return struct{}{}, nil }, // bogus decoder
			func(context.Context, interface{}) ([]*message.Message, error) {
				return nil, expectedErr
			},
			watermilltransport.SubscriberErrorEncoder(errorEncoder(errChan)),
		),
	)

	go func() {
		router.Run(context.Background())
	}()

	go publishSimpleMessage(publisher, "test", []byte("message"))
	err := <-errChan
	if err != expectedErr {
		t.Errorf("want %s, have %s", expectedErr, err)
	}
}

// TestSubscriberNoPublishSuccess checks if NoPublishHandle works properly.
func TestSubscriberNoPublishSuccess(t *testing.T) {
	publisher, subscriber := createPersistentPubSub(t)
	resultChan := make(chan []*message.Message)
	router, _ := watermilltransport.NewRouter(message.RouterConfig{}, nopLogger, subscriber, publisher)
	router.NoPublishHandle(
		"add",
		watermilltransport.NewSubscriber(
			addEndpoint,
			decodeRequest,
			func(ctx context.Context, resp interface{}) (msgs []*message.Message, err error) {
				result := resp.(int)
				msgs = append(msgs, message.NewMessage(watermill.NewShortUUID(), []byte(strconv.Itoa(result))))
				resultChan <- msgs
				return
			},
		),
	)

	go func() {
		router.Run(context.Background())
	}()

	testData := testStruct{A: 5, B: 10}
	go publishMessage(publisher, "add", testData)
	msgs := <-resultChan
	if len(msgs) != 1 {
		t.Errorf("want 1, have %d", len(msgs))
	}

	expectedResult := testData.A + testData.B
	result, _ := strconv.Atoi(string(msgs[0].Payload))
	if result != expectedResult {
		t.Errorf("want %d, have %d", expectedResult, result)
	}
}

// TestSubscriberPublishSuccess checks if Handle works properly.
func TestSubscriberPublishSuccess(t *testing.T) {
	publisher, subscriber := createPersistentPubSub(t)
	router, _ := watermilltransport.NewRouter(message.RouterConfig{}, nopLogger, subscriber, publisher)
	router.Handle(
		"add.input",
		"add.output",
		watermilltransport.NewSubscriber(
			addEndpoint,
			decodeRequest,
			func(ctx context.Context, resp interface{}) (msgs []*message.Message, err error) {
				result := resp.(int)
				msgs = append(msgs, message.NewMessage(watermill.NewShortUUID(), []byte(strconv.Itoa(result))))
				return
			},
		),
	)

	// handle published result and pass the result to resultChan
	resultChan := make(chan int)
	router.NoPublishHandle(
		"add.output",
		watermilltransport.NewSubscriber(
			func(ctx context.Context, req interface{}) (interface{}, error) {
				data := req.(int)
				resultChan <- data
				return struct{}{}, nil
			},
			func(ctx context.Context, msg *message.Message) (interface{}, error) {
				return strconv.Atoi(string(msg.Payload))
			},
			nil, // encode
		),
	)

	go func() {
		router.Run(context.Background())
	}()

	testData := testStruct{A: 4, B: 3}
	go publishMessage(publisher, "add.input", testData)

	expectedResult := testData.A + testData.B
	result := <-resultChan
	if result != expectedResult {
		t.Errorf("want %d, have %d", expectedResult, result)
	}
}

func addEndpoint(ctx context.Context, req interface{}) (interface{}, error) {
	data := req.(testStruct)
	return data.A + data.B, nil
}

func decodeRequest(ctx context.Context, msg *message.Message) (interface{}, error) {
	var req testStruct
	err := json.Unmarshal(msg.Payload, &req)
	return req, err
}

func createPersistentPubSub(t *testing.T) (message.Publisher, message.Subscriber) {
	pubSub := gochannel.NewGoChannel(
		gochannel.Config{
			OutputChannelBuffer: 10,
			Persistent:          true,
		},
		nopLogger,
	)
	return pubSub, pubSub
}

func publishSimpleMessage(publisher message.Publisher, topic string, payload []byte) error {
	messageToPublish := message.NewMessage(watermill.NewShortUUID(), []byte(payload))
	return publisher.Publish(topic, messageToPublish)
}

func publishMessage(publisher message.Publisher, topic string, data testStruct) error {
	message, err := json.Marshal(data)
	if err != nil {
		return err
	}

	return publishSimpleMessage(publisher, topic, message)
}

func errorEncoder(errChan chan error) watermilltransport.ErrorEncoder {
	return func(ctx context.Context, err error, msgs []*message.Message) {
		errChan <- err
	}
}
