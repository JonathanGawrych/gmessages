// SPDX-License-Identifier: AGPL-3.0-or-later

package libgm

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"

	"google.golang.org/protobuf/proto"

	"go.mau.fi/mautrix-gmessages/pkg/libgm/events"
	"go.mau.fi/mautrix-gmessages/pkg/libgm/gmproto"
)

// QueuedRPC contains one complete unsolicited RPC. Streams has one entry per child.
// The stored payload contains decrypted message content and must remain private.
type QueuedRPC struct {
	ResponseID string
	Digest     [32]byte
	Payload    []byte
	Streams    []string
}

type queuedRPCPayload struct {
	Raw       []byte `json:"raw"`
	Decrypted []byte `json:"decrypted"`
	IsOld     bool   `json:"is_old"`
}

func (msg *IncomingRPCMessage) Queue() (*QueuedRPC, error) {
	if msg.ResponseID == "" {
		return nil, errors.New("unsolicited RPC has empty response ID")
	}
	raw, err := proto.Marshal(msg.IncomingRPCMessage)
	if err != nil {
		return nil, err
	}
	payload, err := json.Marshal(queuedRPCPayload{Raw: raw, Decrypted: msg.DecryptedData, IsOld: msg.IsOld})
	if err != nil {
		return nil, err
	}
	// ResponseID is Google's ACK identity. Timing fields in the outer envelope and
	// IsOld (derived from this connection's startup count) are not logical content.
	var content proto.Message
	switch {
	case msg.Pair != nil:
		content = msg.Pair
	case msg.Gaia != nil:
		content = msg.Gaia
	case msg.Message != nil:
		message := proto.Clone(msg.Message).(*gmproto.RPCMessageData)
		if msg.DecryptedData != nil {
			message.EncryptedData = nil
			message.EncryptedData2 = nil
			message.UnencryptedData = msg.DecryptedData
		}
		content = message
	default:
		return nil, errors.New("unsolicited RPC has no decoded content")
	}
	logical, err := (proto.MarshalOptions{Deterministic: true}).Marshal(content)
	if err != nil {
		return nil, err
	}
	contentType := byte(0)
	if msg.Message != nil && msg.Message.EncryptedData2 != nil {
		contentType = 2
	}
	digest := sha256.Sum256(append([]byte{byte(msg.BugleRoute), contentType}, logical...))
	return &QueuedRPC{ResponseID: msg.ResponseID, Digest: digest, Payload: payload, Streams: msg.queueStreams()}, nil
}

func (msg *IncomingRPCMessage) queueStreams() []string {
	if data, ok := msg.DecryptedMessage.(*gmproto.UpdateEvents); ok {
		switch event := data.Event.(type) {
		case *gmproto.UpdateEvents_ConversationEvent:
			streams := make([]string, len(event.ConversationEvent.GetData()))
			for index, part := range event.ConversationEvent.GetData() {
				streams[index] = "conversation/" + part.GetConversationID()
			}
			return streams
		case *gmproto.UpdateEvents_MessageEvent:
			streams := make([]string, len(event.MessageEvent.GetData()))
			for index, part := range event.MessageEvent.GetData() {
				streams[index] = "conversation/" + part.GetConversationID()
			}
			return streams
		case *gmproto.UpdateEvents_TypingEvent:
			return []string{"conversation/" + event.TypingEvent.GetData().GetConversationID()}
		}
	}
	return []string{"control"}
}

func decodeQueuedRPC(payload []byte) (*IncomingRPCMessage, error) {
	var stored queuedRPCPayload
	if err := json.Unmarshal(payload, &stored); err != nil {
		return nil, err
	}
	msg := &IncomingRPCMessage{IncomingRPCMessage: &gmproto.IncomingRPCMessage{}, IsOld: stored.IsOld, DecryptedData: stored.Decrypted}
	if err := proto.Unmarshal(stored.Raw, msg.IncomingRPCMessage); err != nil {
		return nil, err
	}
	var destination proto.Message
	switch msg.BugleRoute {
	case gmproto.BugleRoute_PairEvent:
		msg.Pair = &gmproto.RPCPairData{}
		destination = msg.Pair
	case gmproto.BugleRoute_GaiaEvent:
		msg.Gaia = &gmproto.RPCGaiaData{}
		destination = msg.Gaia
	case gmproto.BugleRoute_DataEvent:
		msg.Message = &gmproto.RPCMessageData{}
		destination = msg.Message
	default:
		return nil, fmt.Errorf("unknown queued RPC route %d", msg.BugleRoute)
	}
	if err := proto.Unmarshal(msg.MessageData, destination); err != nil {
		return nil, err
	}
	if msg.Message != nil && msg.Message.EncryptedData2 == nil {
		if template, ok := responseType[msg.Message.Action]; ok {
			msg.DecryptedMessage = template.ProtoReflect().New().Interface()
			if err := proto.Unmarshal(msg.DecryptedData, msg.DecryptedMessage); err != nil {
				return nil, err
			}
		}
	}
	return msg, nil
}

// DispatchQueuedRPC returns after the event callback, which may only enqueue an
// event in a portal. It does not confirm a Matrix send or a message database save.
func (c *Client) DispatchQueuedRPC(payload []byte, index int, handler func(any) error) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			// Panic values can include message content. Report the type only.
			err = fmt.Errorf("queued RPC handler panicked (%T)", recovered)
		}
	}()
	if c.evHandler == nil {
		return errors.New("queued RPC has no event handler")
	}
	msg, err := decodeQueuedRPC(payload)
	if err != nil {
		return err
	}
	if index < 0 || index >= len(msg.queueStreams()) {
		return fmt.Errorf("invalid queued RPC child index %d", index)
	}
	if data, ok := msg.DecryptedMessage.(*gmproto.UpdateEvents); ok {
		switch event := data.Event.(type) {
		case *gmproto.UpdateEvents_ConversationEvent:
			event.ConversationEvent.Data = event.ConversationEvent.Data[index : index+1]
		case *gmproto.UpdateEvents_MessageEvent:
			event.MessageEvent.Data = event.MessageEvent.Data[index : index+1]
		}
	}
	// Receipt identity handles replay. The small in-memory dedup cache is updated
	// before callbacks and must not suppress a retry after callback failure.
	msg.Queued = true
	msg.queuedHandler = handler
	c.dispatchRPC(msg)
	return msg.dispatchError
}

func (c *Client) dispatchRPC(msg *IncomingRPCMessage) {
	c.dispatchAccountChange(msg)
	switch msg.BugleRoute {
	case gmproto.BugleRoute_PairEvent:
		c.handlePairingEvent(msg)
	case gmproto.BugleRoute_GaiaEvent:
		c.handleGaiaPairingEvent(msg)
	case gmproto.BugleRoute_DataEvent:
		c.handleUpdatesEvent(msg)
	}
}

func (c *Client) dispatchAccountChange(msg *IncomingRPCMessage) {
	if msg.Message != nil && msg.Message.EncryptedData2 != nil {
		var container gmproto.EncryptedData2Container
		if proto.Unmarshal(msg.DecryptedData, &container) == nil && container.GetAccountChange() != nil {
			change := container.GetAccountChange()
			if strings.ContainsRune(change.GetAccount(), '@') {
				msg.triggerEvent(c, &events.AccountChange{AccountChangeOrSomethingEvent: change, IsFake: true})
			}
		}
	}
}

func (msg *IncomingRPCMessage) triggerEvent(client *Client, event any) {
	if msg.dispatchError != nil {
		return
	} else if msg.queuedHandler != nil {
		msg.dispatchError = msg.queuedHandler(event)
	} else {
		client.triggerEvent(event)
	}
}

type PendingRPC struct {
	Sequence   int64
	ResponseID string
	Index      int
	Payload    []byte
}

// IncomingRPCStore is scoped to one login. Enqueue must commit the whole RPC and
// all children atomically, including duplicate detection for completed receipts.
type IncomingRPCStore interface {
	Enqueue(context.Context, *QueuedRPC) error
	Streams(context.Context) ([]string, error)
	Next(context.Context, string) (*PendingRPC, error)
	Complete(context.Context, *PendingRPC) error
}

type IncomingRPCDispatcher struct {
	store      IncomingRPCStore
	handle     func([]byte, int) error
	onError    func(error)
	ctx        context.Context
	cancel     context.CancelFunc
	lock       sync.Mutex
	ingestLock sync.Mutex
	wake       map[string]chan struct{}
	workers    sync.WaitGroup
	readers    sync.WaitGroup
}

type incomingRPCHandler struct {
	dispatcher *IncomingRPCDispatcher
	enqueue    func(*IncomingRPCMessage) error
}

func (handler *incomingRPCHandler) begin() (func(), error) {
	if handler == nil || handler.dispatcher == nil {
		return func() {}, nil
	}
	dispatcher := handler.dispatcher
	dispatcher.lock.Lock()
	defer dispatcher.lock.Unlock()
	if err := dispatcher.ctx.Err(); err != nil {
		return nil, err
	}
	dispatcher.readers.Add(1)
	return dispatcher.readers.Done, nil
}

func (handler *incomingRPCHandler) cancelled() bool {
	return handler != nil && handler.dispatcher != nil && handler.dispatcher.ctx.Err() != nil
}

func NewIncomingRPCDispatcher(ctx context.Context, store IncomingRPCStore, handle func([]byte, int) error, onError func(error)) (*IncomingRPCDispatcher, error) {
	ctx, cancel := context.WithCancel(ctx)
	dispatcher := &IncomingRPCDispatcher{store: store, handle: handle, onError: onError, ctx: ctx, cancel: cancel, wake: make(map[string]chan struct{})}
	streams, err := store.Streams(ctx)
	if err != nil {
		cancel()
		return nil, err
	}
	for _, stream := range streams {
		dispatcher.notify(stream)
	}
	return dispatcher, nil
}

func (dispatcher *IncomingRPCDispatcher) Enqueue(msg *IncomingRPCMessage) error {
	dispatcher.ingestLock.Lock()
	defer dispatcher.ingestLock.Unlock()
	if err := dispatcher.ctx.Err(); err != nil {
		return err
	}
	queued, err := msg.Queue()
	if err != nil {
		return err
	}
	if err = dispatcher.store.Enqueue(dispatcher.ctx, queued); err != nil {
		return err
	}
	for _, stream := range queued.Streams {
		dispatcher.notify(stream)
	}
	return nil
}

func (dispatcher *IncomingRPCDispatcher) notify(stream string) {
	dispatcher.lock.Lock()
	defer dispatcher.lock.Unlock()
	if dispatcher.ctx.Err() != nil {
		return
	}
	wake, exists := dispatcher.wake[stream]
	if !exists {
		wake = make(chan struct{}, 1)
		dispatcher.wake[stream] = wake
		dispatcher.workers.Add(1)
		go dispatcher.run(stream, wake)
	}
	select {
	case wake <- struct{}{}:
	default:
	}
}

func (dispatcher *IncomingRPCDispatcher) run(stream string, wake <-chan struct{}) {
	defer dispatcher.workers.Done()
	for {
		select {
		case <-dispatcher.ctx.Done():
			return
		case <-wake:
		}
		for dispatcher.ctx.Err() == nil {
			err := dispatcher.processNext(stream)
			if errors.Is(err, errIncomingStreamEmpty) {
				break
			} else if err != nil {
				if dispatcher.ctx.Err() == nil {
					dispatcher.onError(err)
				}
				// Keep this row and all later rows. A later enqueue or reconnect
				// retries the stream, without an unbounded retry loop.
				break
			}
		}
	}
}

var errIncomingStreamEmpty = errors.New("incoming RPC stream is empty")

func (dispatcher *IncomingRPCDispatcher) processNext(stream string) error {
	pending, err := dispatcher.store.Next(dispatcher.ctx, stream)
	if err != nil {
		return fmt.Errorf("load queued RPC: %w", err)
	} else if pending == nil {
		return errIncomingStreamEmpty
	}
	if err = dispatcher.handle(pending.Payload, pending.Index); err != nil {
		return fmt.Errorf("handle queued RPC: %w", err)
	} else if err = dispatcher.ctx.Err(); err != nil {
		return err
	}
	if err = dispatcher.store.Complete(dispatcher.ctx, pending); err != nil {
		return fmt.Errorf("complete queued RPC: %w", err)
	}
	return nil
}

// Stop waits for callbacks that are already running. A replacement dispatcher
// must not start until this returns. Cancellation leaves their rows for replay.
func (dispatcher *IncomingRPCDispatcher) Stop() {
	dispatcher.lock.Lock()
	dispatcher.cancel()
	dispatcher.lock.Unlock()
	dispatcher.readers.Wait()
	// An enqueue may have committed just before cancellation. Wait for it before
	// a replacement enumerates streams, so that committed rows cannot be missed.
	dispatcher.ingestLock.Lock()
	dispatcher.ingestLock.Unlock()
	dispatcher.workers.Wait()
}
