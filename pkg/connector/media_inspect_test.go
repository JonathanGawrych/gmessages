// SPDX-License-Identifier: AGPL-3.0-or-later

package connector

import (
	"context"
	"errors"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"testing/synctest"

	"github.com/rs/zerolog"
	"google.golang.org/protobuf/proto"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/bridgeconfig"
	"maunium.net/go/mautrix/bridgev2/commands"
	"maunium.net/go/mautrix/bridgev2/networkid"

	"go.mau.fi/mautrix-gmessages/pkg/libgm/gmproto"
)

func TestInspectMediaFreshFieldsAndPortalOrder(t *testing.T) {
	previous := bridgev2.EventHandlingTimeoutTicks
	defer func() { bridgev2.EventHandlingTimeoutTicks = previous }()
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		fixture := newRetryFixture(t, ctx)
		prepareMediaRequestFixture(t, ctx, fixture)
		before := fixture.parts(t, ctx)
		// Saved source has no ID. The fresh result deliberately differs.
		fresh := proto.Clone(fixture.message).(*gmproto.Message)
		media := fresh.MessageInfo[1].GetMediaContent()
		media.MediaID = "private-full-id"
		media.DecryptionKey = []byte("private-full-key")
		media.ThumbnailMediaID = "private-thumbnail-id"
		media.ThumbnailDecryptionKey = []byte("private-thumbnail-key")
		media.MediaName = "private-filename.png"
		media.MediaData = []byte("private-inline")
		calls := 0
		reports := make(chan string, 1)
		inspection := &mediaInspectionEvent{
			client: fixture.client, key: fixture.portal.PortalKey, messageID: "fixture.message", partID: "b", eventID: "$b",
			fetch: func(_ context.Context, conversationID string, count int64, cursor *gmproto.Cursor) (*gmproto.ListMessagesResponse, error) {
				calls++
				if conversationID != "chat" || count != 100 || cursor != nil {
					t.Fatal("inspection exceeded the exact conversation/page request")
				}
				return &gmproto.ListMessagesResponse{Messages: []*gmproto.Message{fresh}}, nil
			},
			reply: func(_ context.Context, report string) { reports <- report },
		}
		blocked, unblock := make(chan struct{}), make(chan struct{})
		fixture.bridge.QueueRemoteEvent(fixture.login, orderingTestEvent{key: fixture.portal.PortalKey, handle: func() { close(blocked); <-unblock }})
		<-blocked
		result := fixture.bridge.QueueRemoteEvent(fixture.login, inspection)
		synctest.Wait()
		if !result.Queued || calls != 0 || inspection.callbackRan.Load() {
			t.Fatal("inspection overtook an earlier portal event")
		}
		close(unblock)
		report := <-reports
		if calls != 1 || !strings.Contains(report, "stored_pending=true stored_has_media_id=false has_full_id=true has_full_key=true has_thumbnail_id=true has_thumbnail_key=true inline_bytes=14") {
			t.Fatalf("fresh field presence was not reported: %s", report)
		}
		if !regexp.MustCompile(`^([a-z_]+=(true|false|[0-9]+) ?)+$`).MatchString(report) || strings.Contains(report, "private") {
			t.Fatal("inspection exposed values other than booleans and counts")
		}
		if !reflect.DeepEqual(before, fixture.parts(t, ctx)) || len(fixture.intent.edits) != 0 || len(fixture.converted) != 0 || fixture.client.Client != nil {
			t.Fatal("inspection changed metadata, edited/transferred media, or used a phone action client")
		}
		cancel()
		synctest.Wait()
	})
}

func TestInspectMediaValidationAndFailure(t *testing.T) {
	previous := bridgev2.EventHandlingTimeoutTicks
	defer func() { bridgev2.EventHandlingTimeoutTicks = previous }()
	for _, name := range []string{"wrong portal", "wrong account", "wrong event", "text part", "Matrix event mismatch", "wrong message", "wrong timestamp", "wrong conversation", "wrong part", "duplicate part", "stale text", "fetch error", "cancelled"} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				fixture := newRetryFixture(t, ctx)
				inspection := &mediaInspectionEvent{client: fixture.client, key: fixture.portal.PortalKey, messageID: "fixture.message", partID: "b", eventID: "$b"}
				switch name {
				case "wrong portal":
					inspection.key.ID = "fixture.other"
				case "wrong account":
					fixture.client.Meta.IDPrefix = "other"
				case "wrong event":
					inspection.eventID = "$other"
				case "text part", "stale text":
					for _, part := range fixture.parts(t, ctx) {
						if name == "text part" && part.PartID == "b" {
							part.Metadata.(*MessageMetadata).MediaPartID = ""
						} else if name == "stale text" && part.PartID == "" {
							part.Metadata.(*MessageMetadata).TextHash = "different"
						}
						if err := fixture.bridge.DB.Message.Update(ctx, part); err != nil {
							t.Fatal(err)
						}
					}
				case "Matrix event mismatch":
					fixture.intent.original.ID = "$different"
				case "wrong message":
					fixture.message.MessageID = "other"
				case "wrong timestamp":
					fixture.message.Timestamp++
				case "wrong conversation":
					fixture.message.ConversationID = "other"
				case "wrong part":
					fixture.message.MessageInfo[1].ActionMessageID = proto.String("other")
				case "duplicate part":
					fixture.message.MessageInfo[2].ActionMessageID = proto.String("b")
				}
				before := fixture.parts(t, ctx)
				calls := 0
				inspection.fetch = func(context.Context, string, int64, *gmproto.Cursor) (*gmproto.ListMessagesResponse, error) {
					calls++
					if name == "fetch error" {
						return nil, errors.New("private transport response")
					}
					return &gmproto.ListMessagesResponse{Messages: []*gmproto.Message{fixture.message}}, nil
				}
				lookupCtx, cancelLookup := context.WithCancel(ctx)
				defer cancelLookup()
				if name == "cancelled" {
					cancelLookup()
				}
				if report := inspection.inspectStoredMedia(lookupCtx, fixture.portal); report != "lookup_succeeded=false" {
					t.Fatalf("failed inspection leaked source data or claimed success: %s", report)
				}
				wantCalls := 1
				switch name {
				case "wrong portal", "wrong account", "wrong event", "text part", "Matrix event mismatch", "cancelled":
					wantCalls = 0
				}
				if calls != wantCalls || !reflect.DeepEqual(before, fixture.parts(t, ctx)) || len(fixture.intent.edits) != 0 || len(fixture.converted) != 0 {
					t.Fatal("inspection fetched after failed validation or changed stored state")
				}
				cancel()
				synctest.Wait()
			})
		})
	}
}

func TestInspectMediaAdminAndPortalValidation(t *testing.T) {
	intent := &retryTestIntent{}
	log := zerolog.Nop()
	commandInspectMedia.Run(&commands.Event{User: &bridgev2.User{}, Bot: intent, Log: &log, Ctx: context.Background(), Bridge: &bridgev2.Bridge{Config: &bridgeconfig.BridgeConfig{CommandPrefix: "!gm"}}})
	if len(intent.replies) != 1 || !strings.Contains(intent.replies[0], "admin") {
		t.Fatal("inspection did not reject a non-admin")
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
		commandInspectMedia.Run(&commands.Event{User: fixture.login.User, Bot: fixture.intent, Log: &log, Ctx: ctx, Bridge: fixture.bridge, Portal: other, Args: []string{"$b"}})
		if len(fixture.intent.replies) != 1 || !strings.Contains(fixture.intent.replies[0], "not a stored message in this portal") {
			t.Fatal("inspection did not reject the wrong portal")
		}
		cancel()
		synctest.Wait()
	})
}
