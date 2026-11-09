// SPDX-License-Identifier: AGPL-3.0-or-later

package connector

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"testing/synctest"

	"github.com/rs/zerolog"
	"go.mau.fi/util/exsync"
	"google.golang.org/protobuf/proto"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/bridgeconfig"
	"maunium.net/go/mautrix/bridgev2/commands"
	"maunium.net/go/mautrix/bridgev2/networkid"

	"go.mau.fi/mautrix-gmessages/pkg/libgm"
	"go.mau.fi/mautrix-gmessages/pkg/libgm/gmproto"
)

func prepareMediaRequestFixture(t *testing.T, ctx context.Context, fixture *retryFixture) *mediaRequestEvent {
	t.Helper()
	for _, part := range fixture.parts(t, ctx) {
		if part.PartID == "b" {
			part.Metadata.(*MessageMetadata).MediaID = ""
			if err := fixture.bridge.DB.Message.Update(ctx, part); err != nil {
				t.Fatal(err)
			}
		}
	}
	media := fixture.message.MessageInfo[1].GetMediaContent()
	media.MediaID = ""
	media.MediaData = []byte("fixture inline preview")
	fixture.intent.original.Content.Raw[rawMessageDataKey] = encodeRetryFixture(t, fixture.message)
	return &mediaRequestEvent{client: fixture.client, key: fixture.portal.PortalKey, messageID: "fixture.message", partID: "b", eventID: "$b"}
}

func TestRequestMediaAcknowledgmentAndPortalOrder(t *testing.T) {
	previous := bridgev2.EventHandlingTimeoutTicks
	defer func() { bridgev2.EventHandlingTimeoutTicks = previous }()
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		fixture := newRetryFixture(t, ctx)
		request := prepareMediaRequestFixture(t, ctx, fixture)
		before := fixture.parts(t, ctx)
		calls := 0
		request.request = func(_ context.Context, messageID, partID string) (*gmproto.GetFullSizeImageResponse, error) {
			calls++
			if messageID != "message" || partID != "b" {
				t.Fatalf("requested a different attachment: %q %q", messageID, partID)
			}
			return &gmproto.GetFullSizeImageResponse{}, nil
		}
		reports := make(chan string, 1)
		request.reply = func(_ context.Context, message string) { reports <- message }
		blocked := make(chan struct{})
		unblock := make(chan struct{})
		fixture.bridge.QueueRemoteEvent(fixture.login, orderingTestEvent{key: fixture.portal.PortalKey, handle: func() { close(blocked); <-unblock }})
		<-blocked
		fixture.bridge.QueueRemoteEvent(fixture.login, request)
		synctest.Wait()
		if calls != 0 || request.callbackRan.Load() {
			t.Fatal("phone request overtook an earlier portal event")
		}
		close(unblock)
		report := <-reports
		if calls != 1 || !request.callbackRan.Load() || !strings.Contains(report, "acknowledged") || !strings.Contains(report, "does not confirm") {
			t.Fatalf("incorrect request result: %d %s", calls, report)
		}
		if !reflect.DeepEqual(before, fixture.parts(t, ctx)) || len(fixture.intent.edits) != 0 || len(fixture.converted) != 0 {
			t.Fatal("request changed stored metadata or emitted/transferred media")
		}
		if len(fixture.intent.reads) != 1 || fixture.intent.reads[0] != "$a" {
			t.Fatalf("did not read first part raw data: %v", fixture.intent.reads)
		}
		if (&gmproto.GetFullSizeImageResponse{}).ProtoReflect().Descriptor().Fields().Len() != 0 {
			t.Fatal("full-size response contract changed; review acknowledgment reporting")
		}
		cancel()
		synctest.Wait()
	})
}

func TestRequestMediaValidationAndFailure(t *testing.T) {
	previous := bridgev2.EventHandlingTimeoutTicks
	defer func() { bridgev2.EventHandlingTimeoutTicks = previous }()
	for _, name := range []string{"wrong portal", "wrong account", "no selected part", "successful part", "known ID", "no part ID", "no raw", "stale raw", "wrong raw part", "duplicate raw part", "raw known ID", "read error", "request error", "cancelled"} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				fixture := newRetryFixture(t, ctx)
				request := prepareMediaRequestFixture(t, ctx, fixture)
				switch name {
				case "wrong portal":
					request.key.ID = "fixture.other"
				case "wrong account":
					fixture.client.Meta.IDPrefix = "other"
				case "no selected part":
					request.partID = "other"
				case "successful part", "known ID", "no part ID":
					for _, part := range fixture.parts(t, ctx) {
						if part.PartID != "b" {
							continue
						}
						metadata := part.Metadata.(*MessageMetadata)
						if name == "successful part" {
							metadata.MediaPending = false
						}
						if name == "known ID" {
							metadata.MediaID = "b"
						}
						if name == "no part ID" {
							metadata.MediaPartID = ""
						}
						if err := fixture.bridge.DB.Message.Update(ctx, part); err != nil {
							t.Fatal(err)
						}
					}
				case "no raw":
					fixture.intent.original.Content.Raw = nil
				case "stale raw":
					fixture.message.MessageID = "other"
				case "wrong raw part":
					fixture.message.MessageInfo[1].ActionMessageID = proto.String("other")
				case "duplicate raw part":
					fixture.message.MessageInfo = append(fixture.message.MessageInfo, proto.Clone(fixture.message.MessageInfo[1]).(*gmproto.MessageInfo))
				case "raw known ID":
					fixture.message.MessageInfo[1].GetMediaContent().ThumbnailMediaID = "b"
				case "read error":
					fixture.intent.readError = errors.New("fixture read failure")
				}
				if name != "no raw" {
					fixture.intent.original.Content.Raw[rawMessageDataKey] = encodeRetryFixture(t, fixture.message)
				}
				before := fixture.parts(t, ctx)
				calls := 0
				request.request = func(context.Context, string, string) (*gmproto.GetFullSizeImageResponse, error) {
					calls++
					return nil, errors.New("fixture request failure")
				}
				var report string
				request.reply = func(_ context.Context, message string) { report = message }
				requestCtx := ctx
				if name == "cancelled" {
					var cancelRequest context.CancelFunc
					requestCtx, cancelRequest = context.WithCancel(ctx)
					cancelRequest()
				}
				request.PreHandle(requestCtx, fixture.portal)
				if !request.callbackRan.Load() {
					t.Fatal("callback was not recorded, including its failure/cancellation path")
				}
				wantCalls := 0
				if name == "request error" {
					wantCalls = 1
				}
				if calls != wantCalls || !strings.HasPrefix(report, "Attachment request failed:") {
					t.Fatalf("incorrect failure result: %d %s", calls, report)
				}
				if !reflect.DeepEqual(before, fixture.parts(t, ctx)) || len(fixture.intent.edits) != 0 {
					t.Fatal("failure changed pending state or emitted a Matrix edit")
				}
				cancel()
				synctest.Wait()
			})
		})
	}
}

func TestMediaCommandsSynchronousFailureReporting(t *testing.T) {
	previousBuffer := bridgev2.PortalEventBuffer
	previousTimeout := bridgev2.EventHandlingTimeoutTicks
	defer func() {
		bridgev2.PortalEventBuffer = previousBuffer
		bridgev2.EventHandlingTimeoutTicks = previousTimeout
	}()
	bridgev2.PortalEventBuffer = 0
	for _, command := range []*commands.FullHandler{commandRetryMedia, commandRequestMedia} {
		for _, cancelled := range []bool{false, true} {
			t.Run(command.Name+"/"+map[bool]string{false: "handled", true: "enqueue_cancelled"}[cancelled], func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					fixture := newRetryFixture(t, ctx)
					prepareMediaRequestFixture(t, ctx, fixture)
					fixture.login.User.Permissions.Admin = true
					fixture.login.Client = fixture.client
					// No transport or session handler is installed: validation must fail before any phone call.
					fixture.client.Client = &libgm.Client{AuthData: &libgm.AuthData{Browser: &gmproto.Device{}}}
					fixture.intent.original.Content.Raw = nil
					target := "$a"
					if command == commandRequestMedia {
						target = "$b"
					}
					if cancelled {
						cancel()
						synctest.Wait()
					}
					log := zerolog.Nop()
					command.Run(&commands.Event{User: fixture.login.User, Bot: fixture.intent, Log: &log, Ctx: context.Background(), Bridge: fixture.bridge, Portal: fixture.portal, Args: []string{target}})
					if len(fixture.intent.replies) != 2 {
						t.Fatalf("duplicate or missing result notice: %v", fixture.intent.replies)
					}
					final := fixture.intent.replies[1]
					if cancelled {
						if !strings.Contains(final, "could not be queued") || len(fixture.intent.reads) != 0 {
							t.Fatalf("cancelled enqueue ran a callback or lacked failure notice: %v", fixture.intent.replies)
						}
					} else if !strings.Contains(final, "raw message data is unavailable") || strings.Contains(final, "could not be queued") {
						t.Fatalf("handled validation failure was reported as an enqueue failure: %v", fixture.intent.replies)
					}
					cancel()
					synctest.Wait()
				})
			})
		}
	}
}

func TestRequestMediaCommandGates(t *testing.T) {
	intent := &retryTestIntent{}
	log := zerolog.Nop()
	commandRequestMedia.Run(&commands.Event{User: &bridgev2.User{Permissions: bridgeconfig.Permissions{}}, Bot: intent, Log: &log, Ctx: context.Background(), Bridge: &bridgev2.Bridge{Config: &bridgeconfig.BridgeConfig{CommandPrefix: "!gm"}}})
	if len(intent.replies) != 1 || !strings.Contains(intent.replies[0], "admin") {
		t.Fatalf("non-admin response: %v", intent.replies)
	}
	previous := bridgev2.EventHandlingTimeoutTicks
	defer func() { bridgev2.EventHandlingTimeoutTicks = previous }()
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		fixture := newRetryFixture(t, ctx)
		other, err := fixture.bridge.GetPortalByKey(ctx, networkid.PortalKey{ID: "fixture.other"})
		if err != nil {
			t.Fatal(err)
		}
		fixture.login.User.Permissions.Admin = true
		commandRequestMedia.Run(&commands.Event{User: fixture.login.User, Bot: fixture.intent, Log: &log, Ctx: ctx, Bridge: fixture.bridge, Portal: other, Args: []string{"$b"}})
		if len(fixture.intent.replies) != 1 || !strings.Contains(fixture.intent.replies[0], "not a stored message in this portal") {
			t.Fatalf("wrong-portal response: %v", fixture.intent.replies)
		}
		cancel()
		synctest.Wait()
	})
}

func TestRequestedMediaUpdateUsesNativeEditPath(t *testing.T) {
	previous := bridgev2.EventHandlingTimeoutTicks
	defer func() { bridgev2.EventHandlingTimeoutTicks = previous }()
	for _, status := range []gmproto.MessageStatusType{gmproto.MessageStatusType_INCOMING_COMPLETE, gmproto.MessageStatusType_OUTGOING_COMPLETE} {
		t.Run(status.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				fixture := newRetryFixture(t, ctx)
				fixture.message.MessageStatus.Status = status
				fixture.message.MessageInfo = fixture.message.MessageInfo[:1]
				fixture.message.MessageInfo[0].GetMediaContent().MediaID = ""
				for _, part := range fixture.parts(t, ctx) {
					if part.PartID != "" {
						if err := fixture.bridge.DB.Message.Delete(ctx, part.RowID); err != nil {
							t.Fatal(err)
						}
						continue
					}
					metadata := part.Metadata.(*MessageMetadata)
					metadata.IsOutgoing = status == gmproto.MessageStatusType_OUTGOING_COMPLETE
					metadata.Type = status
					metadata.MediaID = ""
					metadata.GlobalPartCount = 1
					metadata.GlobalMediaStatus = downloadPendingStatusMessage(status)
					if err := fixture.bridge.DB.Message.Update(ctx, part); err != nil {
						t.Fatal(err)
					}
				}
				fixture.client.chatInfoCache = exsync.NewMap[string, *gmproto.Conversation]()
				incoming := &MessageEvent{WrappedMessage: &libgm.WrappedMessage{Message: fixture.message}, g: fixture.client}
				if !incoming.hasInProgressMedia() {
					t.Fatal("no-ID attachment was not initially pending")
				}
				fixture.message.MessageInfo[0].GetMediaContent().MediaID = "new-media"
				existing := fixture.parts(t, ctx)
				if incoming.hasInProgressMedia() {
					t.Fatal("updated attachment still classified as in progress")
				}
				if edited, _ := DBMessages(existing).MessageWasEdited(fixture.message); status == gmproto.MessageStatusType_OUTGOING_COMPLETE && edited {
					t.Fatal("outgoing fixture must rely on pending completion, not the media-ID change detector")
				}
				result, err := incoming.HandleExisting(ctx, fixture.portal, fixture.intent, existing)
				if err != nil {
					t.Fatal(err)
				}
				if len(result.SubEvents) == 0 {
					t.Fatal("media update did not generate native sub-events")
				}
				if _, ok := result.SubEvents[0].(*MessageUpdateEvent); !ok {
					t.Fatalf("media update did not use native edit: %T", result.SubEvents[0])
				}
				for _, part := range fixture.parts(t, ctx) {
					if !part.Metadata.(*MessageMetadata).MediaPending {
						t.Fatal("planning an edit cleared pending before transfer/send")
					}
				}
				if status == gmproto.MessageStatusType_OUTGOING_COMPLETE {
					existing[0].Metadata.(*MessageMetadata).MediaPending = false
					withoutPending, err := incoming.HandleExisting(ctx, fixture.portal, fixture.intent, existing)
					if err != nil {
						t.Fatal(err)
					}
					for _, subEvent := range withoutPending.SubEvents {
						if _, ok := subEvent.(*MessageUpdateEvent); ok {
							t.Fatal("outgoing control case triggered an edit without pending completion")
						}
					}
				}
				cancel()
				synctest.Wait()
			})
		})
	}
}
