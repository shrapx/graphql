package graphql

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
)

// Subscription transport follow Apollo's subscriptions-transport-ws protocol specification
// https://github.com/apollographql/subscriptions-transport-ws/blob/master/PROTOCOL.md

// OperationMessageType
type OperationMessageType string

const (
	GQL_CONNECTION_INIT OperationMessageType = "connection_init"
	GQL_START           OperationMessageType = "start"
	GQL_STOP            OperationMessageType = "stop"
	GQL_ERROR           OperationMessageType = "error"
	GQL_DATA            OperationMessageType = "data"
)

type OperationMessage struct {
	ID      string               `json:"id,omitempty"`
	Type    OperationMessageType `json:"type"`
	Payload json.RawMessage      `json:"payload,omitempty"`
}

// WebsocketHandler abstracts WebSocket connecton functions
// ReadJSON and WriteJSON data of a frame from the WebSocket connection.
// Close the WebSocket connection.
type WebsocketHandler interface {
	ReadJSON(v interface{}) error
	WriteJSON(v interface{}) error
	Close() error
}

// SubscriptionClient is a GraphQL subscription client.
type SubscriptionClient struct {
	conn             WebsocketHandler
	connectionParams map[string]interface{}
	context          context.Context
	subscriptions    map[string]func(data *json.RawMessage, err error)
	OnError          func(sc *SubscriptionClient, err error)
}

func NewSubscriptionClient(conn WebsocketHandler, connectionParams map[string]interface{}) *SubscriptionClient {
	ctx, _ := context.WithCancel(context.Background())
	return &SubscriptionClient{
		conn:             conn,
		connectionParams: connectionParams,
		context:          ctx,
		subscriptions:    make(map[string]func(data *json.RawMessage, err error)),
	}
}

func (sc *SubscriptionClient) init() error {
	// send connection_init event to the server
	msg := OperationMessage{
		Type:    GQL_CONNECTION_INIT,
		Payload: sc.connectionParams,
	}

	return sc.conn.WriteJSON(msg)
}

// Subscribe sends start message to server and open a channel to receive data
func (sc *SubscriptionClient) Subscribe(v interface{}, variables map[string]interface{}, handler func(message json.RawMessage, err error)) (string, error) {
	id := uuid.New()
	query := constructSubscription(v, variables)

	in := struct {
		Query     string                 `json:"query"`
		Variables map[string]interface{} `json:"variables,omitempty"`
	}{
		Query:     query,
		Variables: variables,
	}
	// send stop message to the server
	msg := OperationMessage{
		ID:      id,
		Type:    GQL_START,
		Payload: in,
	}
	if err := sc.conn.WriteJSON(msg); err != nil {
		return "", err
	}

	sc.subscriptions[id] = handler

	return id, nil
}

func (sc *SubscriptionClient) Run() error {

	for {
		var message OperationMessage
		if err := sc.conn.ReadJSON(&message); err != nil {
			if sc.OnError != nil {
				sc.OnError(sc, err)
			} else {
				sc.Close()
				return err
			}
		}

		switch message.Type {
		case GQL_DATA:
			id, err := uuid.FromString(message.ID)
			if err != nil {
				continue
			}
			c, ok := sc.subscriptions[id]
			if !ok {
				continue
			}

			var out struct {
				Data   *json.RawMessage
				Errors errors
				//Extensions interface{} // Unused.
			}
			err = json.Unmarshal(message.Payload, &out)
			if err != nil {
				// TODO: Consider including response body in returned error, if deemed helpful.
				c.handler(nil, err)
				continue
			}
			if len(out.Errors) > 0 {
				c.handler(nil, err)
				continue
			}

			c.handler(out.Data, nil)
		}
	}
}

// Stop sends stop message to server and close subscription channel
func (sc *SubscriptionClient) Stop(id string) error {
	_, ok := sc.subscriptions[id]
	if !ok {
		return fmt.Errorf("subscription id %s doesn't not exist", id)
	}
	// send stop message to the server
	msg := OperationMessage{
		ID:   id,
		Type: GQL_STOP,
	}

	if err := sc.conn.WriteJSON(msg); err != nil {
		return nil
	}

	delete(sc.subscriptions, id)
	return nil
}

// Close closes all subscription channel and websocket as well
func (sc *SubscriptionClient) Close() error {
	for id := range sc.subscriptions {
		if err := sc.Stop(id); err != nil {
			return err
		}
	}
	<-sc.context.Done()

	return sc.conn.Close()
}
