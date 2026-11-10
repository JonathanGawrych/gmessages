// SPDX-License-Identifier: AGPL-3.0-or-later

package connector

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"google.golang.org/protobuf/proto"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/id"

	"go.mau.fi/mautrix-gmessages/pkg/libgm/gmproto"
)

func TestMediaLookupRawStates(t *testing.T) {
	previous := bridgev2.EventHandlingTimeoutTicks
	defer func() { bridgev2.EventHandlingTimeoutTicks = previous }()
	for _, name := range []string{"absent", "empty", "valid", "null", "wrong type", "invalid base64", "oversized", "mismatch", "wrong Matrix event", "Matrix read failure", "wrong account"} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				fixture := newRetryFixture(t, ctx)
				parts := fixture.parts(t, ctx)
				switch name {
				case "absent":
					fixture.intent.original.Content.Raw = nil
				case "empty":
					fixture.intent.original.Content.Raw[rawMessageDataKey] = ""
				case "null":
					fixture.intent.original.Content.Raw[rawMessageDataKey] = nil
				case "wrong type":
					fixture.intent.original.Content.Raw[rawMessageDataKey] = 1
				case "invalid base64":
					fixture.intent.original.Content.Raw[rawMessageDataKey] = "!"
				case "oversized":
					fixture.intent.original.Content.Raw[rawMessageDataKey] = strings.Repeat("A", 8192)
				case "mismatch":
					other := proto.Clone(fixture.message).(*gmproto.Message)
					other.MessageID = "other"
					fixture.intent.original.Content.Raw[rawMessageDataKey] = encodeRetryFixture(t, other)
				case "wrong Matrix event":
					fixture.intent.original.Sender = "@other:example.invalid"
					fixture.intent.original.Content.Raw = nil
				case "Matrix read failure":
					fixture.intent.readError = errors.New("fixture read failure")
				case "wrong account":
					fixture.client.Meta.IDPrefix = "other"
					fixture.intent.original.Content.Raw = nil
				}
				calls := 0
				fetch := func(context.Context, string, int64, *gmproto.Cursor) (*gmproto.ListMessagesResponse, error) {
					calls++
					return &gmproto.ListMessagesResponse{Messages: []*gmproto.Message{fixture.message}}, nil
				}
				_, message, encoded, err := fixture.client.loadMediaMessage(ctx, fixture.portal, fixture.intent, "fixture.message", parts, fetch)
				wantLookup := name == "absent" || name == "empty"
				wantSuccess := wantLookup || name == "valid"
				if (err == nil) != wantSuccess || (calls == 1) != wantLookup || calls > 1 {
					t.Fatalf("raw validation result: calls=%d error=%v", calls, err)
				}
				if wantSuccess && !proto.Equal(message, fixture.message) {
					t.Fatal("loaded a different message")
				}
				if wantLookup && encoded != "" {
					t.Fatal("lookup fabricated saved Matrix raw data")
				}
				cancel()
				synctest.Wait()
			})
		})
	}
}

func TestMediaLookupSinglePageAndIdentity(t *testing.T) {
	previous := bridgev2.EventHandlingTimeoutTicks
	defer func() { bridgev2.EventHandlingTimeoutTicks = previous }()
	for _, name := range []string{"newer anchor", "no newer anchor", "wrong anchor account", "empty page", "not found", "wrong conversation", "wrong timestamp", "duplicate", "oversized page", "stale text", "stale count", "fetch error", "cancel before", "cancel during"} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				fixture := newRetryFixture(t, ctx)
				fixture.intent.original.Content.Raw = nil
				parts := fixture.parts(t, ctx)
				baseTime := time.UnixMicro(fixture.message.Timestamp)
				if name == "newer anchor" || name == "wrong anchor account" {
					for index, offset := range []time.Duration{0, time.Microsecond, 2 * time.Millisecond, time.Millisecond} {
						messageID := networkid.MessageID("fixture.anchor" + string(rune('a'+index)))
						if name == "wrong anchor account" && index == 3 {
							messageID = "other.anchor"
						}
						if err := fixture.bridge.DB.Message.Insert(ctx, &database.Message{ID: messageID, MXID: id.EventID("$anchor" + string(rune('a'+index))), Room: fixture.portal.PortalKey, Timestamp: baseTime.Add(offset), Metadata: &MessageMetadata{}}); err != nil {
							t.Fatal(err)
						}
					}
				}
				candidate := proto.Clone(fixture.message).(*gmproto.Message)
				response := &gmproto.ListMessagesResponse{Messages: []*gmproto.Message{candidate}, Cursor: &gmproto.Cursor{LastItemID: "never-follow", LastItemTimestamp: 1}}
				switch name {
				case "empty page":
					response.Messages = nil
				case "not found":
					candidate.MessageID = "other"
				case "wrong conversation":
					candidate.ConversationID = "other"
				case "wrong timestamp":
					candidate.Timestamp++
				case "duplicate":
					response.Messages = append(response.Messages, candidate)
				case "oversized page":
					response.Messages = make([]*gmproto.Message, 101)
				case "stale text":
					for _, part := range parts {
						if part.PartID == "" {
							part.Metadata.(*MessageMetadata).TextHash = "different"
						}
					}
				case "stale count":
					candidate.MessageInfo = candidate.MessageInfo[:2]
				}
				lookupCtx, cancelLookup := context.WithCancel(ctx)
				defer cancelLookup()
				if name == "cancel before" {
					cancelLookup()
				}
				calls := 0
				fetch := func(_ context.Context, conversationID string, count int64, cursor *gmproto.Cursor) (*gmproto.ListMessagesResponse, error) {
					calls++
					if calls != 1 || conversationID != "chat" || count != 100 {
						t.Fatal("lookup exceeded the one-page conversation bound")
					}
					if name == "newer anchor" {
						if cursor == nil || cursor.LastItemID != "anchord" || cursor.LastItemTimestamp != baseTime.Add(time.Millisecond).UnixMilli() {
							t.Fatal("lookup did not use the closest actual anchor in a newer millisecond")
						}
					} else if cursor != nil {
						t.Fatal("lookup fabricated a cursor")
					}
					if name == "fetch error" {
						return nil, errors.New("fixture fetch failure")
					}
					if name == "cancel during" {
						cancelLookup()
					}
					return response, nil
				}
				_, _, _, err := fixture.client.loadMediaMessage(lookupCtx, fixture.portal, fixture.intent, "fixture.message", parts, fetch)
				wantSuccess := name == "newer anchor" || name == "no newer anchor"
				wantCalls := 1
				if name == "cancel before" || name == "wrong anchor account" {
					wantCalls = 0
				}
				if (err == nil) != wantSuccess || calls != wantCalls {
					t.Fatalf("bounded lookup: calls=%d error=%v", calls, err)
				}
				if len(fixture.intent.edits) != 0 || len(fixture.converted) != 0 {
					t.Fatal("lookup emitted or converted messages")
				}
				cancel()
				synctest.Wait()
			})
		})
	}
}

func TestRetryMediaLookupNativePartialRecovery(t *testing.T) {
	previous := bridgev2.EventHandlingTimeoutTicks
	defer func() { bridgev2.EventHandlingTimeoutTicks = previous }()
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		fixture := newRetryFixture(t, ctx)
		fixture.intent.original.Content.Raw = nil
		fixture.message.MessageInfo[0].GetMediaContent().MediaData = make([]byte, 9000)
		calls := 0
		fixture.fetch = func(context.Context, string, int64, *gmproto.Cursor) (*gmproto.ListMessagesResponse, error) {
			calls++
			return &gmproto.ListMessagesResponse{Messages: []*gmproto.Message{fixture.message}}, nil
		}
		fixture.convertFailure = "b"
		if report := fixture.enqueue(t); !strings.Contains(report, "1 remain pending") {
			t.Fatalf("partial lookup retry: %s", report)
		}
		fixture.convertFailure = ""
		if report := fixture.enqueue(t); !strings.Contains(report, "Retried 1 pending") {
			t.Fatalf("second lookup retry: %s", report)
		}
		if calls != 2 || !reflect.DeepEqual(fixture.converted, []string{"a", "b", "b"}) || len(fixture.intent.edits) != 2 {
			t.Fatal("lookup retry repeated successful transfers or exceeded one page per command")
		}
		for _, part := range fixture.parts(t, ctx) {
			if part.Metadata.(*MessageMetadata).MediaPending {
				t.Fatal("successful native replacement was not stored")
			}
		}
		cancel()
		synctest.Wait()
	})
}

func TestRequestMediaLookupValidation(t *testing.T) {
	previous := bridgev2.EventHandlingTimeoutTicks
	defer func() { bridgev2.EventHandlingTimeoutTicks = previous }()
	for _, name := range []string{"no ID", "fetched known ID", "wrong part", "wrong sender"} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				fixture := newRetryFixture(t, ctx)
				request := prepareMediaRequestFixture(t, ctx, fixture)
				fixture.intent.original.Content.Raw = nil
				if name == "fetched known ID" {
					fixture.message.MessageInfo[1].GetMediaContent().MediaID = "new-full-size"
				} else if name == "wrong part" {
					fixture.message.MessageInfo[1].ActionMessageID = proto.String("other")
				} else if name == "wrong sender" {
					for _, part := range fixture.parts(t, ctx) {
						if part.PartID == "b" {
							part.SenderMXID = "@other:example.invalid"
							if err := fixture.bridge.DB.Message.Update(ctx, part); err != nil {
								t.Fatal(err)
							}
						}
					}
				}
				before := fixture.parts(t, ctx)
				fetches, requests := 0, 0
				request.fetch = func(context.Context, string, int64, *gmproto.Cursor) (*gmproto.ListMessagesResponse, error) {
					fetches++
					return &gmproto.ListMessagesResponse{Messages: []*gmproto.Message{fixture.message}}, nil
				}
				request.request = func(_ context.Context, messageID, partID string) (*gmproto.GetFullSizeImageResponse, error) {
					requests++
					if messageID != "message" || partID != "b" {
						t.Fatal("requested a different attachment")
					}
					return &gmproto.GetFullSizeImageResponse{}, nil
				}
				var report string
				request.reply = func(_ context.Context, result string) { report = result }
				request.PreHandle(ctx, fixture.portal)
				if fetches != 1 || (requests == 1) != (name == "no ID") {
					t.Fatalf("lookup/request counts: %d/%d", fetches, requests)
				}
				if name == "fetched known ID" && !strings.Contains(report, "does not reconcile that state") {
					t.Fatalf("known ID was not explicitly reported: %s", report)
				}
				if !reflect.DeepEqual(before, fixture.parts(t, ctx)) || len(fixture.intent.edits) != 0 || len(fixture.converted) != 0 {
					t.Fatal("request lookup changed pending state, transferred media or edited Matrix")
				}
				cancel()
				synctest.Wait()
			})
		})
	}
}
