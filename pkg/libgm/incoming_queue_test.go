// SPDX-License-Identifier: AGPL-3.0-or-later

package libgm

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"google.golang.org/protobuf/proto"

	"go.mau.fi/mautrix-gmessages/pkg/libgm/crypto"
	"go.mau.fi/mautrix-gmessages/pkg/libgm/events"
	"go.mau.fi/mautrix-gmessages/pkg/libgm/gmproto"
)

type queueTestStore struct {
	lock      sync.Mutex
	receipts  map[string]*QueuedRPC
	pending   map[int64]*PendingRPC
	streams   map[int64]string
	sequence  int64
	fail      bool
	completed chan string
}

func newQueueTestStore() *queueTestStore {
	return &queueTestStore{receipts: make(map[string]*QueuedRPC), pending: make(map[int64]*PendingRPC), streams: make(map[int64]string), completed: make(chan string, 20)}
}

func (store *queueTestStore) Enqueue(ctx context.Context, queued *QueuedRPC) error {
	store.lock.Lock()
	defer store.lock.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if store.fail {
		return errors.New("fixture enqueue failure")
	}
	if previous := store.receipts[queued.ResponseID]; previous != nil {
		if previous.Digest != queued.Digest {
			return errors.New("changed RPC")
		}
		return nil
	}
	store.receipts[queued.ResponseID] = queued
	for index, stream := range queued.Streams {
		store.sequence++
		store.pending[store.sequence] = &PendingRPC{Sequence: store.sequence, ResponseID: queued.ResponseID, Index: index, Payload: queued.Payload}
		store.streams[store.sequence] = stream
	}
	return nil
}

func (store *queueTestStore) Streams(context.Context) ([]string, error) {
	store.lock.Lock()
	defer store.lock.Unlock()
	var streams []string
	for sequence := range store.pending {
		if !slices.Contains(streams, store.streams[sequence]) {
			streams = append(streams, store.streams[sequence])
		}
	}
	return streams, nil
}

func (store *queueTestStore) Next(ctx context.Context, stream string) (*PendingRPC, error) {
	store.lock.Lock()
	defer store.lock.Unlock()
	var first *PendingRPC
	for sequence, pending := range store.pending {
		if store.streams[sequence] == stream && (first == nil || sequence < first.Sequence) {
			first = pending
		}
	}
	return first, ctx.Err()
}

func (store *queueTestStore) Complete(ctx context.Context, pending *PendingRPC) error {
	store.lock.Lock()
	defer store.lock.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	delete(store.pending, pending.Sequence)
	store.completed <- pending.ResponseID
	return nil
}

func queueTestClient() *Client {
	client := NewClient(&AuthData{RequestCrypto: crypto.NewAESCTRHelper()}, nil, zerolog.Nop())
	client.SetEventHandler(func(any) {})
	return client
}

func queueTestRaw(t *testing.T, client *Client, id string, action gmproto.ActionType, message proto.Message) *gmproto.IncomingRPCMessage {
	t.Helper()
	data, err := proto.Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	ciphertext, err := client.AuthData.RequestCrypto.Encrypt(data)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := proto.Marshal(&gmproto.RPCMessageData{Action: action, SessionID: "request", EncryptedData: ciphertext})
	if err != nil {
		t.Fatal(err)
	}
	return &gmproto.IncomingRPCMessage{ResponseID: id, BugleRoute: gmproto.BugleRoute_DataEvent, MessageData: raw}
}

func queueTestMessages(messages ...*gmproto.Message) *gmproto.UpdateEvents {
	return &gmproto.UpdateEvents{Event: &gmproto.UpdateEvents_MessageEvent{MessageEvent: &gmproto.MessageEvent{Data: messages}}}
}

func queueTestAwait[T any](t *testing.T, channel <-chan T) T {
	t.Helper()
	select {
	case value := <-channel:
		return value
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for fixture event")
	}
	var zero T
	return zero
}

func queueTestHandle(t *testing.T, client *Client, raw *gmproto.IncomingRPCMessage) {
	t.Helper()
	if err := client.HandleRPCMsg(raw); err != nil {
		t.Fatal(err)
	}
}

func TestIncomingRPCBlockedConversationDoesNotBlockReplyOrOtherConversation(t *testing.T) {
	client := queueTestClient()
	store := newQueueTestStore()
	started := make(chan struct{})
	release := make(chan struct{})
	delivered := make(chan string, 10)
	errorsSeen := make(chan error, 10)
	dispatcher, err := NewIncomingRPCDispatcher(context.Background(), store, func(payload []byte, index int) error {
		return client.DispatchQueuedRPC(payload, index, func(event any) error {
			switch event := event.(type) {
			case *WrappedMessage:
				if event.MessageID == "a1" {
					close(started)
					<-release
				}
				delivered <- event.MessageID
			case *gmproto.Conversation:
				delivered <- "conversation/" + event.ConversationID
			case *gmproto.Settings:
				delivered <- "control"
			}
			return nil
		})
	}, func(err error) { errorsSeen <- err })
	if err != nil {
		t.Fatal(err)
	}
	defer dispatcher.Stop()
	client.SetIncomingRPCDispatcher(dispatcher)
	first := queueTestRaw(t, client, "first", gmproto.ActionType_GET_UPDATES, queueTestMessages(&gmproto.Message{MessageID: "a1", ConversationID: "a"}, &gmproto.Message{MessageID: "b1", ConversationID: "b"}))
	queueTestHandle(t, client, first)
	queueTestAwait(t, started)
	if got := queueTestAwait(t, delivered); got != "b1" {
		t.Fatalf("other conversation = %q", got)
	}
	settings := &gmproto.UpdateEvents{Event: &gmproto.UpdateEvents_SettingsEvent{SettingsEvent: &gmproto.Settings{}}}
	queueTestHandle(t, client, queueTestRaw(t, client, "control", gmproto.ActionType_GET_UPDATES, settings))
	if got := queueTestAwait(t, delivered); got != "control" {
		t.Fatalf("control stream = %q", got)
	}
	conversation := &gmproto.UpdateEvents{Event: &gmproto.UpdateEvents_ConversationEvent{ConversationEvent: &gmproto.ConversationEvent{Data: []*gmproto.Conversation{{ConversationID: "a"}}}}}
	queueTestHandle(t, client, queueTestRaw(t, client, "conversation", gmproto.ActionType_GET_UPDATES, conversation))
	queueTestHandle(t, client, queueTestRaw(t, client, "second", gmproto.ActionType_GET_UPDATES, queueTestMessages(&gmproto.Message{MessageID: "a2", ConversationID: "a"})))
	waiter := client.sessionHandler.waitResponse("request")
	queueTestHandle(t, client, queueTestRaw(t, client, "reply", gmproto.ActionType_LIST_MESSAGES, &gmproto.ListMessagesResponse{}))
	if reply := queueTestAwait(t, waiter); reply.ResponseID != "reply" {
		t.Fatal("wrong command reply")
	}
	// A replay after one child completed must not recreate that child.
	queueTestHandle(t, client, first)
	close(release)
	for _, expected := range []string{"a1", "conversation/a", "a2"} {
		if got := queueTestAwait(t, delivered); got != expected {
			t.Fatalf("got %q, want %q", got, expected)
		}
	}
	for range 5 {
		queueTestAwait(t, store.completed)
	}
	select {
	case err := <-errorsSeen:
		t.Fatal(err)
	default:
	}
	select {
	case extra := <-delivered:
		t.Fatalf("duplicate child %q", extra)
	default:
	}
}

func TestIncomingRPCCommandResponsePreservesAccountChange(t *testing.T) {
	client := queueTestClient()
	data, err := proto.Marshal(&gmproto.EncryptedData2Container{AccountChange: &gmproto.AccountChangeOrSomethingEvent{Account: "fixture@example.invalid"}})
	if err != nil {
		t.Fatal(err)
	}
	encrypted, err := client.AuthData.RequestCrypto.Encrypt(data)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := proto.Marshal(&gmproto.RPCMessageData{Action: gmproto.ActionType_NOTIFY_DITTO_ACTIVITY, SessionID: "request", EncryptedData2: encrypted})
	if err != nil {
		t.Fatal(err)
	}
	changes := make(chan *events.AccountChange, 1)
	client.SetEventHandler(func(event any) { changes <- event.(*events.AccountChange) })
	client.incomingRPCHandler.Store(&incomingRPCHandler{enqueue: func(*IncomingRPCMessage) error {
		t.Fatal("matched command response entered unsolicited queue")
		return nil
	}})
	waiter := client.sessionHandler.waitResponse("request")
	queueTestHandle(t, client, &gmproto.IncomingRPCMessage{ResponseID: "reply", BugleRoute: gmproto.BugleRoute_DataEvent, MessageData: raw})
	queueTestAwait(t, waiter)
	if change := queueTestAwait(t, changes); !change.IsFake || change.GetAccount() != "fixture@example.invalid" {
		t.Fatal("matched response lost account-change side effect")
	}
}

func TestIncomingRPCAcknowledgementRequiresSuccessfulEnqueue(t *testing.T) {
	client := queueTestClient()
	raw := queueTestRaw(t, client, "event", gmproto.ActionType_GET_UPDATES, queueTestMessages(&gmproto.Message{MessageID: "one", ConversationID: "a"}))
	committed := false
	client.incomingRPCHandler.Store(&incomingRPCHandler{enqueue: func(msg *IncomingRPCMessage) error {
		if slices.Contains(client.sessionHandler.ackMap, msg.ResponseID) {
			t.Fatal("ACK preceded commit")
		}
		if !committed {
			return errors.New("database unavailable")
		}
		return nil
	}})
	if err := client.HandleRPCMsg(raw); err == nil || len(client.sessionHandler.ackMap) != 0 {
		t.Fatal("failed enqueue was acknowledged")
	}
	committed = true
	queueTestHandle(t, client, raw)
	if !slices.Contains(client.sessionHandler.ackMap, "event") {
		t.Fatal("committed event was not acknowledged")
	}
	raw.MessageData = []byte{0xff}
	if err := client.HandleRPCMsg(raw); err == nil {
		t.Fatal("malformed RPC accepted")
	}
}

func TestIncomingRPCStopWaitsAndRestartReplaysCancelledCallback(t *testing.T) {
	client := queueTestClient()
	store := newQueueTestStore()
	msg, err := client.decryptInternalMessage(queueTestRaw(t, client, "pending", gmproto.ActionType_GET_UPDATES, queueTestMessages(&gmproto.Message{MessageID: "one", ConversationID: "a"})))
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	release := make(chan struct{})
	dispatcher, err := NewIncomingRPCDispatcher(context.Background(), store, func([]byte, int) error { close(started); <-release; return nil }, func(err error) { t.Error(err) })
	if err != nil {
		t.Fatal(err)
	}
	if err = dispatcher.Enqueue(msg); err != nil {
		t.Fatal(err)
	}
	queueTestAwait(t, started)
	stopped := make(chan struct{})
	go func() { dispatcher.Stop(); close(stopped) }()
	queueTestAwait(t, dispatcher.ctx.Done())
	select {
	case <-stopped:
		t.Fatal("stopped before blocked callback returned")
	default:
	}
	close(release)
	queueTestAwait(t, stopped)
	pending, _ := store.Next(context.Background(), "conversation/a")
	if pending == nil {
		t.Fatal("cancelled callback deleted its row")
	}
	restarted, err := NewIncomingRPCDispatcher(context.Background(), store, func(payload []byte, index int) error {
		return client.DispatchQueuedRPC(payload, index, func(any) error { return nil })
	}, func(err error) { t.Error(err) })
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Stop()
	queueTestAwait(t, store.completed)
	if err = restarted.Enqueue(msg); err != nil {
		t.Fatal(err)
	}
	pending, _ = store.Next(context.Background(), "conversation/a")
	if pending != nil {
		t.Fatal("completed RPC replay recreated a child")
	}
}

func TestIncomingRPCHandlerFailureRetainsRowAndDoesNotDeduplicateRetry(t *testing.T) {
	for _, panicHandler := range []bool{false, true} {
		t.Run(fmt.Sprint(panicHandler), func(t *testing.T) {
			client := queueTestClient()
			store := newQueueTestStore()
			msg, err := client.decryptInternalMessage(queueTestRaw(t, client, "pending", gmproto.ActionType_GET_UPDATES, queueTestMessages(&gmproto.Message{MessageID: "one", ConversationID: "a"})))
			if err != nil {
				t.Fatal(err)
			}
			failure := make(chan error, 2)
			attempts := 0
			dispatcher, err := NewIncomingRPCDispatcher(context.Background(), store, func(payload []byte, index int) error {
				return client.DispatchQueuedRPC(payload, index, func(any) error {
					attempts++
					if attempts == 1 {
						if panicHandler {
							panic("fixture failure")
						}
						return errors.New("fixture failure")
					}
					return nil
				})
			}, func(err error) { failure <- err })
			if err != nil {
				t.Fatal(err)
			}
			defer dispatcher.Stop()
			if err = dispatcher.Enqueue(msg); err != nil {
				t.Fatal(err)
			}
			queueTestAwait(t, failure)
			pending, _ := store.Next(context.Background(), "conversation/a")
			if pending == nil {
				t.Fatal("failure deleted pending event")
			}
			if err = dispatcher.Enqueue(msg); err != nil {
				t.Fatal(err)
			}
			queueTestAwait(t, store.completed)
			if attempts != 2 {
				t.Fatalf("attempts = %d", attempts)
			}
		})
	}
}

func TestIncomingRPCReplayDigestAndStoredOldFlag(t *testing.T) {
	client := queueTestClient()
	update := queueTestMessages(&gmproto.Message{MessageID: "one", ConversationID: "a"})
	first, err := client.decryptInternalMessage(queueTestRaw(t, client, "same", gmproto.ActionType_GET_UPDATES, update))
	if err != nil {
		t.Fatal(err)
	}
	second, err := client.decryptInternalMessage(queueTestRaw(t, client, "same", gmproto.ActionType_GET_UPDATES, update))
	if err != nil {
		t.Fatal(err)
	}
	second.IsOld = true
	second.Timestamp = "999"
	a, err := first.Queue()
	if err != nil {
		t.Fatal(err)
	}
	b, err := second.Queue()
	if err != nil {
		t.Fatal(err)
	}
	if a.Digest != b.Digest {
		t.Fatal("encryption randomness, timestamp or IsOld changed logical digest")
	}
	store := newQueueTestStore()
	if err = store.Enqueue(context.Background(), a); err != nil {
		t.Fatal(err)
	}
	if err = store.Enqueue(context.Background(), b); err != nil {
		t.Fatal(err)
	}
	pending, _ := store.Next(context.Background(), "conversation/a")
	if err = client.DispatchQueuedRPC(pending.Payload, pending.Index, func(event any) error {
		if event.(*WrappedMessage).IsOld {
			t.Fatal("replay replaced original IsOld=false")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err = client.DispatchQueuedRPC([]byte("invalid"), 0, nil); err == nil {
		t.Fatal("invalid payload accepted")
	}
}

func TestIncomingRPCRetiredReaderCannotEnterReplacementDispatcher(t *testing.T) {
	client := queueTestClient()
	store := newQueueTestStore()
	old, err := NewIncomingRPCDispatcher(context.Background(), store, func([]byte, int) error { return nil }, func(err error) { t.Error(err) })
	if err != nil {
		t.Fatal(err)
	}
	client.SetIncomingRPCDispatcher(old)
	readerHandler := client.incomingRPCHandler.Load()
	old.Stop()
	replacement, err := NewIncomingRPCDispatcher(context.Background(), store, func([]byte, int) error { return nil }, func(err error) { t.Error(err) })
	if err != nil {
		t.Fatal(err)
	}
	defer replacement.Stop()
	client.SetIncomingRPCDispatcher(replacement)
	client.skipCount = 7
	deadline := time.Now().Add(-time.Hour)
	client.nextDataReceiveCheck = deadline
	raw := queueTestRaw(t, client, "buffered", gmproto.ActionType_GET_UPDATES, queueTestMessages(&gmproto.Message{MessageID: "one", ConversationID: "a"}))
	if err = client.handleRPCMsg(raw, readerHandler); !errors.Is(err, context.Canceled) {
		t.Fatalf("retired reader: %v", err)
	}
	if len(client.sessionHandler.ackMap) != 0 {
		t.Fatal("retired reader acknowledged its uncommitted RPC")
	}
	if client.skipCount != 7 || !client.nextDataReceiveCheck.Equal(deadline) {
		t.Fatal("retired reader changed replacement connection skip count or receive timer")
	}
	store.lock.Lock()
	count := len(store.receipts)
	store.lock.Unlock()
	if count != 0 {
		t.Fatal("retired reader entered replacement dispatcher")
	}
	listenID := client.listenID.Load()
	if client.doLongPoll(false, false, nil, readerHandler) || client.listenID.Load() != listenID {
		t.Fatal("retired goroutine started a new polling connection")
	}
	queueTestHandle(t, client, raw)
	queueTestAwait(t, store.completed)
}

type queueTestTransport func(*http.Request) (*http.Response, error)

func (transport queueTestTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	return transport(request)
}

type queueTestLogWriter func([]byte) (int, error)

func (writer queueTestLogWriter) Write(data []byte) (int, error) { return writer(data) }

type queueTestPollingBody struct {
	readPrefix bool
	waiting    chan struct{}
	closed     chan struct{}
	release    chan struct{}
	closeOnce  sync.Once
}

func (body *queueTestPollingBody) Read(destination []byte) (int, error) {
	if !body.readPrefix {
		body.readPrefix = true
		return copy(destination, "[["), nil
	}
	close(body.waiting)
	<-body.closed
	return 0, io.EOF
}

func (body *queueTestPollingBody) Close() error {
	body.closeOnce.Do(func() { close(body.closed) })
	<-body.release
	return nil
}

func TestIncomingRPCStopJoinsOuterPollingCleanup(t *testing.T) {
	client := queueTestClient()
	dispatcher, err := NewIncomingRPCDispatcher(context.Background(), newQueueTestStore(), func([]byte, int) error { return nil }, func(err error) { t.Error(err) })
	if err != nil {
		t.Fatal(err)
	}
	client.SetIncomingRPCDispatcher(dispatcher)
	body := &queueTestPollingBody{waiting: make(chan struct{}), closed: make(chan struct{}), release: make(chan struct{})}
	var requests atomic.Int32
	client.lphttp = &http.Client{Transport: queueTestTransport(func(*http.Request) (*http.Response, error) {
		requests.Add(1)
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: body}, nil
	})}
	done := make(chan bool, 1)
	go func() { done <- client.doLongPoll(false, false, nil, client.incomingRPCHandler.Load()) }()
	queueTestAwait(t, body.waiting)
	stopped := make(chan struct{})
	go func() { dispatcher.Stop(); close(stopped) }()
	queueTestAwait(t, body.closed)
	select {
	case <-stopped:
		t.Fatal("Stop returned before the outer polling connection cleanup")
	default:
	}
	close(body.release)
	queueTestAwait(t, stopped)
	if client.longPollingConn.Load() != nil || requests.Load() != 1 {
		t.Fatal("retired outer loop retained its connection or issued another request")
	}
	replacement := &longPollingConnection{Closer: io.NopCloser(strings.NewReader(""))}
	client.longPollingConn.Store(replacement)
	queueTestAwait(t, done)
	if client.longPollingConn.Load() != replacement {
		t.Fatal("retired outer loop cleared the replacement connection")
	}
}

func TestIncomingRPCStopCancelsPollingRequestAndRetryWait(t *testing.T) {
	for _, retryWait := range []bool{false, true} {
		t.Run(fmt.Sprint(retryWait), func(t *testing.T) {
			client := queueTestClient()
			dispatcher, err := NewIncomingRPCDispatcher(context.Background(), newQueueTestStore(), func([]byte, int) error { return nil }, func(err error) { t.Error(err) })
			if err != nil {
				t.Fatal(err)
			}
			client.SetIncomingRPCDispatcher(dispatcher)
			waiting := make(chan struct{})
			var signalOnce sync.Once
			if retryWait {
				client.Logger = zerolog.New(queueTestLogWriter(func(data []byte) (int, error) {
					if strings.Contains(string(data), "Error making listen request, retrying in a while") {
						signalOnce.Do(func() { close(waiting) })
					}
					return len(data), nil
				}))
			}
			var requests atomic.Int32
			client.lphttp = &http.Client{Transport: queueTestTransport(func(request *http.Request) (*http.Response, error) {
				requests.Add(1)
				if retryWait {
					return nil, errors.New("fixture transport failure")
				}
				signalOnce.Do(func() { close(waiting) })
				<-request.Context().Done()
				return nil, request.Context().Err()
			})}
			done := make(chan bool, 1)
			go func() { done <- client.doLongPoll(false, false, nil, client.incomingRPCHandler.Load()) }()
			queueTestAwait(t, waiting)
			// No response body is installed yet, so closing the connection alone
			// cannot interrupt this HTTP request or retry wait.
			client.closeLongPolling()
			stopped := make(chan struct{})
			go func() { dispatcher.Stop(); close(stopped) }()
			queueTestAwait(t, stopped)
			queueTestAwait(t, done)
			if requests.Load() != 1 || client.longPollingConn.Load() != nil {
				t.Fatal("cancelled polling loop resumed HTTP requests")
			}
		})
	}
}
