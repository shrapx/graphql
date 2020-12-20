package graphql

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"nhooyr.io/websocket"
	"nhooyr.io/websocket/wsjson"
)

// Subscription transport follow Apollo's subscriptions-transport-ws protocol specification
// https://github.com/apollographql/subscriptions-transport-ws/blob/master/PROTOCOL.md

// OperationMessageType
type OperationMessageType string

const (
	// Client sends this message after plain websocket connection to start the communication with the server
	GQL_CONNECTION_INIT OperationMessageType = "connection_init"
	// The server may responses with this message to the GQL_CONNECTION_INIT from client, indicates the server rejected the connection.
	GQL_CONNECTION_ERROR OperationMessageType = "connection_error"
	GQL_CONNECTION_ERROR_2 OperationMessageType = "conn_err"
	// Client sends this message to execute GraphQL operation
	GQL_START OperationMessageType = "start"
	// Client sends this message in order to stop a running GraphQL operation execution (for example: unsubscribe)
	GQL_STOP OperationMessageType = "stop"
	// Server sends this message upon a failing operation, before the GraphQL execution, usually due to GraphQL validation errors (resolver errors are part of GQL_DATA message, and will be added as errors array)
	GQL_ERROR OperationMessageType = "error"
	// The server sends this message to transfter the GraphQL execution result from the server to the client, this message is a response for GQL_START message.
	GQL_DATA OperationMessageType = "data"
	// Server sends this message to indicate that a GraphQL operation is done, and no more data will arrive for the specific operation.
	GQL_COMPLETE OperationMessageType = "complete"
	// Server message that should be sent right after each GQL_CONNECTION_ACK processed and then periodically to keep the client connection alive.
	// The client starts to consider the keep alive message only upon the first received keep alive message from the server.
	GQL_CONNECTION_KEEP_ALIVE OperationMessageType = "ka"
	// The server may responses with this message to the GQL_CONNECTION_INIT from client, indicates the server accepted the connection. May optionally include a payload.
	GQL_CONNECTION_ACK OperationMessageType = "connection_ack"
	// Client sends this message to terminate the connection.
	GQL_CONNECTION_TERMINATE OperationMessageType = "connection_terminate"
)

type OperationMessage struct {
	ID      string               `json:"id,omitempty"`
	Type    OperationMessageType `json:"type"`
	Payload json.RawMessage      `json:"payload,omitempty"`
}

func (om OperationMessage) String() string {
	bs, _ := json.Marshal(om)

	return string(bs)
}

// WebsocketHandler abstracts WebSocket connecton functions
// ReadJSON and WriteJSON data of a frame from the WebSocket connection.
// Close the WebSocket connection.
type WebsocketConn interface {
	ReadJSON(v interface{}) error
	WriteJSON(v interface{}) error
	Close() error
	// SetReadLimit sets the maximum size in bytes for a message read from the peer. If a
	// message exceeds the limit, the connection sends a close message to the peer
	// and returns ErrReadLimit to the application.
	SetReadLimit(limit int64)
}

type subscription struct {
	query     string
	variables map[string]interface{}
	handler   func(data json.RawMessage, err error) error
	started   Boolean
}

// SubscriptionClient is a GraphQL subscription client.
type SubscriptionClient struct {
	url                string
	conn               WebsocketConn
	connectionParams   map[string]interface{}
	connectionParamsMu sync.Mutex
	context            context.Context
	subscriptions      map[string]*subscription
	cancel             context.CancelFunc
	subscribersMu      sync.Mutex
	timeout            time.Duration
	isRunning          Boolean
	readLimit          int64 // max size of response message. Default 10 MB
	log                func(args ...interface{})
	createConn         func(sc *SubscriptionClient) (WebsocketConn, error)
	retryTimeout       time.Duration
	onConnected        func()
	onDisconnected     func()
	onError            func(sc *SubscriptionClient, err error) error
}

func NewSubscriptionClient(url string) *SubscriptionClient {
	return &SubscriptionClient{
		url:           url,
		timeout:       time.Minute,
		readLimit:     10 * 1024 * 1024, // set default limit 10MB
		subscriptions: make(map[string]*subscription),
		createConn:    newWebsocketConn,
		retryTimeout:  time.Minute,
	}
}

// GetURL returns GraphQL server's URL
func (sc *SubscriptionClient) GetURL() string {
	return sc.url
}

// GetContext returns current context of subscription client
func (sc *SubscriptionClient) GetContext() context.Context {
	return sc.context
}

// GetContext returns write timeout of websocket client
func (sc *SubscriptionClient) GetTimeout() time.Duration {
	return sc.timeout
}

// WithWebSocket replaces customized websocket client constructor
// In default, subscription client uses https://github.com/nhooyr/websocket
func (sc *SubscriptionClient) WithWebSocket(fn func(sc *SubscriptionClient) (WebsocketConn, error)) *SubscriptionClient {
	sc.createConn = fn
	return sc
}

// WithConnectionParams updates connection params for sending to server through GQL_CONNECTION_INIT event
// It's usually used for authentication handshake
func (sc *SubscriptionClient) WithConnectionParams(params map[string]interface{}) *SubscriptionClient {
	sc.connectionParamsMu.Lock()
	defer sc.connectionParamsMu.Unlock()
	sc.connectionParams = params
	return sc
}

// WithTimeout updates write timeout of websocket client
func (sc *SubscriptionClient) WithTimeout(timeout time.Duration) *SubscriptionClient {
	sc.timeout = timeout
	return sc
}

// WithRetryTimeout updates reconnecting timeout. When the websocket server was stopped, the client will retry connecting every second until timeout
func (sc *SubscriptionClient) WithRetryTimeout(timeout time.Duration) *SubscriptionClient {
	sc.retryTimeout = timeout
	return sc
}

// WithLog sets loging function to print out received messages. By default, nothing is printed
func (sc *SubscriptionClient) WithLog(logger func(args ...interface{})) *SubscriptionClient {
	sc.log = logger
	return sc
}

// WithReadLimit set max size of response message
func (sc *SubscriptionClient) WithReadLimit(limit int64) *SubscriptionClient {
	sc.readLimit = limit
	return sc
}

// OnError event is triggered when there is any connection error. This is bottom exception handler level
// If this function is empty, or returns nil, the error is ignored
// If returns error, the websocket connection will be terminated
func (sc *SubscriptionClient) OnError(onError func(sc *SubscriptionClient, err error) error) *SubscriptionClient {
	sc.onError = onError
	return sc
}

// OnConnected event is triggered when the websocket connected to GraphQL server sucessfully
func (sc *SubscriptionClient) OnConnected(fn func()) *SubscriptionClient {
	sc.onConnected = fn
	return sc
}

// OnDisconnected event is triggered when the websocket server was stil down after retry timeout
func (sc *SubscriptionClient) OnDisconnected(fn func()) *SubscriptionClient {
	sc.onDisconnected = fn
	return sc
}

func (sc *SubscriptionClient) setIsRunning(value Boolean) {
	sc.subscribersMu.Lock()
	sc.isRunning = value
	sc.subscribersMu.Unlock()
}

func (sc *SubscriptionClient) init() error {

	now := time.Now()
	ctx, cancel := context.WithCancel(context.Background())
	sc.context = ctx
	sc.cancel = cancel

	for {
		var err error
		var conn WebsocketConn
		// allow custom websocket client
		if sc.conn == nil {
			conn, err = newWebsocketConn(sc)
			if err != nil {
				err = fmt.Errorf("create websocket failed: %v", err)
			} else {
				sc.conn = conn
			}
		}

		if err == nil {
			sc.conn.SetReadLimit(sc.readLimit)
			// send connection init event to the server
			err = sc.sendConnectionInit()
		}

		if err == nil {
			return nil
		}

		if now.Add(sc.retryTimeout).Before(time.Now()) {
			// close sc.conn on timeout, allows to reconnect
			sc.closeConnection()
			if sc.onDisconnected != nil {
				sc.onDisconnected()
			}
			return err
		}
		if sc.log != nil {
			sc.log(err, "retry in second....")
		}
		time.Sleep(time.Second)
	}
}

func (sc *SubscriptionClient) sendConnectionInit() (err error) {
	bParams, err := sc.marshalConnectionParams()
	if err != nil {
		return
	}

	// send connection_init event to the server
	msg := OperationMessage{
		Type:    GQL_CONNECTION_INIT,
		Payload: bParams,
	}

	return sc.conn.WriteJSON(msg)
}

func (sc *SubscriptionClient) marshalConnectionParams() (params []byte, err error) {
	sc.connectionParamsMu.Lock()
	defer sc.connectionParamsMu.Unlock()
	if sc.connectionParams != nil {
		params, err = json.Marshal(sc.connectionParams)
		if err != nil {
			return
		}
		return params, nil
	}
	return nil, nil
}

// Subscribe sends start message to server and open a channel to receive data.
// The handler callback function will receive raw message data or error. If the call return error, onError event will be triggered
// The function returns subscription ID and error. You can use subscription ID to unsubscribe the subscription
func (sc *SubscriptionClient) Subscribe(v interface{}, variables map[string]interface{}, handler func(message json.RawMessage, err error) error) (string, error) {
	id := uuid.New().String()
	query := constructSubscription(v, variables)

	sub := subscription{
		query:     query,
		variables: variables,
		handler:   handler,
	}

	// if the websocket client is running, start subscription immediately
	if sc.isRunning {
		if err := sc.startSubscription(id, &sub); err != nil {
			return "", err
		}
	}

	sc.subscribersMu.Lock()
	sc.subscriptions[id] = &sub
	sc.subscribersMu.Unlock()

	return id, nil
}

// Subscribe sends start message to server and open a channel to receive data
func (sc *SubscriptionClient) startSubscription(id string, sub *subscription) error {
	if sub == nil || sub.started {
		return nil
	}

	in := struct {
		Query     string                 `json:"query"`
		Variables map[string]interface{} `json:"variables,omitempty"`
	}{
		Query:     sub.query,
		Variables: sub.variables,
	}

	payload, err := json.Marshal(in)
	if err != nil {
		return err
	}

	// send stop message to the server
	msg := OperationMessage{
		ID:      id,
		Type:    GQL_START,
		Payload: payload,
	}

	if err := sc.conn.WriteJSON(msg); err != nil {
		return err
	}

	sub.started = true
	return nil
}

func (sc *SubscriptionClient) handleHandlerError(err error) error {
	if err != nil && sc.onError != nil {
		return sc.onError(sc, err)
	}

	return nil
}

// Run start websocket client and subscriptions. If this function is run with goroutine, it can be stopped after closed
func (sc *SubscriptionClient) Run() error {
	if err := sc.init(); err != nil {
		return fmt.Errorf("retry timeout. exiting...")
	}

	// lazily start subscriptions
	err := sc.startSubscriptions()
	if err != nil {
		return err
	}
	sc.setIsRunning(true)

	for {
		select {
		case <-sc.context.Done():
			return nil
		default:

			var message OperationMessage
			if err := sc.conn.ReadJSON(&message); err != nil {
				// manual EOF check
				if err == io.EOF || strings.Contains(err.Error(), "EOF") {
					sc.setIsRunning(false)
					return nil
				}
				if sc.onError != nil {
					if err = sc.onError(sc, err); err != nil {
						return err
					}
				}
				continue
			}

			if sc.log != nil {
				sc.log(message)
			}

			switch message.Type {
			case GQL_ERROR, GQL_DATA:
				id, err := uuid.Parse(message.ID)
				if err != nil {
					continue
				}

				sc.subscribersMu.Lock()
				sub, ok := sc.subscriptions[id.String()]
				sc.subscribersMu.Unlock()
				if !ok {
					continue
				}
				var out struct {
					Data   json.RawMessage
					Errors errors
					//Extensions interface{} // Unused.
				}

				err = json.Unmarshal(message.Payload, &out)
				if err != nil {
					if err = sc.handleHandlerError(sub.handler(nil, err)); err != nil {
						return err
					}
					continue
				}
				if len(out.Errors) > 0 {
					if err = sc.handleHandlerError(sub.handler(nil, out.Errors)); err != nil {
						return err
					}
					continue
				}

				if err = sc.handleHandlerError(sub.handler(out.Data, nil)); err != nil {
					return err
				}
			case GQL_CONNECTION_ERROR, GQL_CONNECTION_ERROR_2:
			case GQL_COMPLETE:
				sc.Unsubscribe(message.ID)
			case GQL_CONNECTION_KEEP_ALIVE:
			case GQL_CONNECTION_ACK:
				if sc.onConnected != nil {
					sc.onConnected()
				}
			default:
			}
		}
	}
}

func (sc *SubscriptionClient) startSubscriptions() error {
	sc.subscribersMu.Lock()
	defer sc.subscribersMu.Unlock()
	for k, v := range sc.subscriptions {
		if err := sc.startSubscription(k, v); err != nil {
			sc.unsubscribe(k)
			return err
		}
	}
	return nil
}

// Unsubscribe sends stop message to server and close subscription channel
// The input parameter is subscription ID that is returned from Subscribe function
func (sc *SubscriptionClient) Unsubscribe(id string) error {
	sc.subscribersMu.Lock()
	defer sc.subscribersMu.Unlock()
	return sc.unsubscribe(id)
}

func (sc *SubscriptionClient) UnsubscribeAll() {
	sc.subscribersMu.Lock()
	defer sc.subscribersMu.Unlock()
	for id := range sc.subscriptions {
		if err := sc.unsubscribe(id); err != nil {
			sc.log(err.Error()) // log unsubscribe errors
		}
	}
}

func (sc *SubscriptionClient) unsubscribe(id string) error {
	_, ok := sc.subscriptions[id]
	if !ok {
		return fmt.Errorf("subscription id %s does not exist", id)
	}

	err := sc.stopSubscription(id)

	delete(sc.subscriptions, id)
	return err
}

func (sc *SubscriptionClient) stopSubscription(id string) error {
	if sc.conn != nil {
		// send stop message to the server
		msg := OperationMessage{
			ID:   id,
			Type: GQL_STOP,
		}

		if err := sc.conn.WriteJSON(msg); err != nil {
			return err
		}
	}
	return nil
}

func (sc *SubscriptionClient) stopSubscriptions() error {
	if !sc.isRunning {
		return fmt.Errorf("connection is not running")
	}
	sc.subscribersMu.Lock()
	defer sc.subscribersMu.Unlock()
	for id, sub := range sc.subscriptions {
		_ = sc.stopSubscription(id)
		sub.started = false
	}
	return nil
}


func (sc *SubscriptionClient) terminate() error {
	if sc.conn != nil {
		// send terminate message to the server
		msg := OperationMessage{
			Type: GQL_CONNECTION_TERMINATE,
		}

		return sc.conn.WriteJSON(msg)
	}
	return nil
}

func (sc *SubscriptionClient) closeConnection() {
	_ = sc.terminate()
	if sc.conn != nil {
		_ = sc.conn.Close()
		sc.conn = nil
	}
}

// Reset restart websocket connection and subscriptions
func (sc *SubscriptionClient) Reset() {
	_ = sc.stopSubscriptions()
	sc.closeConnection()
	if sc.cancel != nil {
		sc.cancel()
	}
}

// Close closes all subscriptions and websocket as well
func (sc *SubscriptionClient) Close() error {
	sc.setIsRunning(false)
	sc.UnsubscribeAll()
	sc.closeConnection()
	if sc.cancel != nil {
		sc.cancel()
	}
	return nil
}

// default websocket handler implementation using https://github.com/nhooyr/websocket
type websocketHandler struct {
	ctx     context.Context
	timeout time.Duration
	*websocket.Conn
}

func (wh *websocketHandler) WriteJSON(v interface{}) error {
	ctx, cancel := context.WithTimeout(wh.ctx, wh.timeout)
	defer cancel()

	return wsjson.Write(ctx, wh.Conn, v)
}

func (wh *websocketHandler) ReadJSON(v interface{}) error {
	ctx, cancel := context.WithTimeout(wh.ctx, wh.timeout)
	defer cancel()
	return wsjson.Read(ctx, wh.Conn, v)
}

func (wh *websocketHandler) Close() error {
	return wh.Conn.Close(websocket.StatusNormalClosure, "close websocket")
}

func newWebsocketConn(sc *SubscriptionClient) (WebsocketConn, error) {

	options := &websocket.DialOptions{
		Subprotocols: []string{"graphql-ws"},
	}
	c, _, err := websocket.Dial(sc.GetContext(), sc.GetURL(), options)
	if err != nil {
		return nil, err
	}

	return &websocketHandler{
		ctx:     sc.GetContext(),
		Conn:    c,
		timeout: sc.GetTimeout(),
	}, nil
}
