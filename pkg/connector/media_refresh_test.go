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

	"github.com/rs/zerolog"
	"google.golang.org/protobuf/proto"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/bridgeconfig"
	"maunium.net/go/mautrix/bridgev2/commands"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/id"

	"go.mau.fi/mautrix-gmessages/pkg/libgm/gmproto"
)

func (fixture *retryFixture) enqueueRefresh(t *testing.T, partID networkid.PartID, eventID id.EventID) string {
	t.Helper()
	reports := make(chan string, 1)
	refresh := &mediaRefreshEvent{
		mediaRetryEvent: mediaRetryEvent{
			client: fixture.client, key: fixture.portal.PortalKey, messageID: "fixture.message", fetch: fixture.fetch, convert: fixture.convertMedia,
			reply: func(_ context.Context, report string) { reports <- report },
		},
		partID: partID, eventID: eventID,
	}
	if !refresh.GetSender().ForceEditOrigSender {
		t.Fatal("refresh did not retain native original-sender handling")
	}
	if result := fixture.bridge.QueueRemoteEvent(fixture.login, refresh); !result.Queued {
		t.Fatalf("queue refresh: %+v", result)
	}
	return <-reports
}

func TestRefreshMediaOnlySelectedPartAndDuplicateSkip(t *testing.T) {
	previous := bridgev2.EventHandlingTimeoutTicks
	defer func() { bridgev2.EventHandlingTimeoutTicks = previous }()
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		fixture := newRetryFixture(t, ctx)
		prepareMediaRequestFixture(t, ctx, fixture)
		before := fixture.parts(t, ctx)
		saved := fixture.intent.original.Content.Raw[rawMessageDataKey]
		fresh := proto.Clone(fixture.message).(*gmproto.Message)
		fresh.MessageInfo[1].GetMediaContent().MediaID = "fresh-b"
		fresh.MessageInfo[1].GetMediaContent().DecryptionKey = []byte("fixture-key")
		calls := 0
		fixture.fetch = func(_ context.Context, conversationID string, count int64, cursor *gmproto.Cursor) (*gmproto.ListMessagesResponse, error) {
			calls++
			if conversationID != "chat" || count != 100 || cursor != nil {
				t.Fatal("refresh exceeded the bounded conversation lookup")
			}
			return &gmproto.ListMessagesResponse{Messages: []*gmproto.Message{fresh}}, nil
		}
		if report := fixture.enqueueRefresh(t, "b", "$b"); !strings.Contains(report, "Retried 1 pending") {
			t.Fatal(report)
		}
		if report := fixture.enqueueRefresh(t, "b", "$b"); !strings.Contains(report, "already complete; skipped") {
			t.Fatal(report)
		}
		if calls != 1 || !reflect.DeepEqual(fixture.converted, []string{"fresh-b"}) || len(fixture.intent.edits) != 1 || fixture.intent.edits[0].RelatesTo.GetReplaceID() != "$b" {
			t.Fatal("refresh repeated a completed part or changed another part")
		}
		after := fixture.parts(t, ctx)
		for index, part := range after {
			if part.PartID == "b" {
				metadata := *part.Metadata.(*MessageMetadata)
				if metadata.MediaPending || metadata.MediaID != "fresh-b" {
					t.Fatal("fresh replacement was not saved")
				}
				metadata.MediaPending = true
				metadata.MediaID = ""
				part.Metadata = &metadata
			}
			if !reflect.DeepEqual(part, before[index]) {
				t.Fatal("refresh changed identity or unrelated message metadata")
			}
		}
		if fixture.client.Client != nil || saved != fixture.intent.original.Content.Raw[rawMessageDataKey] {
			t.Fatal("refresh used a phone action client or rewrote saved raw data")
		}
		for _, extra := range fixture.intent.editExtras {
			if _, hasRaw := extra[rawMessageDataKey]; hasRaw || extra["m.new_content"] != nil {
				t.Fatal("fresh raw data was copied into a replacement")
			}
		}
		cancel()
		synctest.Wait()
	})
}

func TestRefreshMediaPreservesMergedCaptionAndOrder(t *testing.T) {
	previous := bridgev2.EventHandlingTimeoutTicks
	defer func() { bridgev2.EventHandlingTimeoutTicks = previous }()
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		fixture := newRetryFixture(t, ctx)
		for _, part := range fixture.parts(t, ctx) {
			if part.PartID != "" {
				if err := fixture.bridge.DB.Message.Delete(ctx, part.RowID); err != nil {
					t.Fatal(err)
				}
			} else {
				part.Metadata.(*MessageMetadata).GlobalPartCount = 2
				part.Metadata.(*MessageMetadata).MediaID = ""
				if err := fixture.bridge.DB.Message.Update(ctx, part); err != nil {
					t.Fatal(err)
				}
			}
		}
		fixture.message.MessageInfo = append(fixture.message.MessageInfo[:1], &gmproto.MessageInfo{Data: &gmproto.MessageInfo_MessageContent{MessageContent: &gmproto.MessageContent{Content: "Original caption"}}})
		fixture.message.MessageInfo[0].GetMediaContent().DecryptionKey = []byte("fixture-key")
		fixture.intent.original.Content.Raw = nil
		calls := 0
		fixture.fetch = func(context.Context, string, int64, *gmproto.Cursor) (*gmproto.ListMessagesResponse, error) {
			calls++
			return &gmproto.ListMessagesResponse{Messages: []*gmproto.Message{fixture.message}}, nil
		}
		blocked, release := make(chan struct{}), make(chan struct{})
		fixture.bridge.QueueRemoteEvent(fixture.login, orderingTestEvent{key: fixture.portal.PortalKey, handle: func() { close(blocked); <-release }})
		<-blocked
		finished := make(chan string, 1)
		go func() { finished <- fixture.enqueueRefresh(t, "", "$a") }()
		synctest.Wait()
		if calls != 0 || len(fixture.converted) != 0 {
			t.Fatal("refresh overtook earlier portal events")
		}
		close(release)
		if report := <-finished; !strings.Contains(report, "Retried 1 pending") {
			t.Fatal(report)
		}
		content := fixture.intent.edits[0].NewContent
		if content.Body != "Original caption" || content.FileName != "a.png" || len(fixture.intent.editExtras[0]) != 1 {
			t.Fatal("merged caption changed or fresh raw data was persisted")
		}
		part := fixture.parts(t, ctx)[0]
		if part.SenderMXID != fixture.intent.GetMXID() || part.Timestamp.UnixMicro() != fixture.message.Timestamp || part.MXID != "$a" || part.PartID != "" {
			t.Fatal("refresh changed the original sender or message identity")
		}
		cancel()
		synctest.Wait()
	})
}

func TestRefreshMediaValidationAndFailure(t *testing.T) {
	previous := bridgev2.EventHandlingTimeoutTicks
	defer func() { bridgev2.EventHandlingTimeoutTicks = previous }()
	for _, name := range []string{"no full ID", "no key", "wrong account", "wrong message", "wrong timestamp", "wrong conversation", "wrong part", "duplicate part", "wrong sender", "wrong stored timestamp", "stale text", "fetch error", "cancelled", "upload", "send", "save"} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				fixture := newRetryFixture(t, ctx)
				fresh := proto.Clone(fixture.message).(*gmproto.Message)
				media := fresh.MessageInfo[1].GetMediaContent()
				media.MediaID = "fresh-b"
				media.DecryptionKey = []byte("fixture-key")
				switch name {
				case "no full ID":
					media.MediaID = ""
					media.ThumbnailMediaID = "thumbnail-only"
				case "no key":
					media.DecryptionKey = nil
				case "wrong account":
					fixture.client.Meta.IDPrefix = "other"
				case "wrong message":
					fresh.MessageID = "other"
				case "wrong timestamp":
					fresh.Timestamp++
				case "wrong conversation":
					fresh.ConversationID = "other"
				case "wrong part":
					fresh.MessageInfo[1].ActionMessageID = proto.String("other")
				case "duplicate part":
					fresh.MessageInfo[2].ActionMessageID = proto.String("b")
				case "wrong sender", "wrong stored timestamp", "stale text":
					for _, part := range fixture.parts(t, ctx) {
						if part.PartID == "b" && name == "wrong sender" {
							part.SenderMXID = "@other:example.invalid"
						} else if part.PartID == "b" && name == "wrong stored timestamp" {
							part.Timestamp = part.Timestamp.Add(time.Microsecond)
						} else if part.PartID == "" && name == "stale text" {
							part.Metadata.(*MessageMetadata).TextHash = "different"
						}
						if err := fixture.bridge.DB.Message.Update(ctx, part); err != nil {
							t.Fatal(err)
						}
					}
				case "upload":
					fixture.convertFailure = "fresh-b"
				case "send":
					fixture.intent.failTarget = "$b"
				case "save":
					if _, err := fixture.bridge.DB.Exec(ctx, `CREATE TRIGGER fail_refresh_save BEFORE UPDATE ON message WHEN NEW.part_id='b' BEGIN SELECT RAISE(FAIL, 'fixture save failure'); END`); err != nil {
						t.Fatal(err)
					}
				}
				before := fixture.parts(t, ctx)
				fixture.fetch = func(context.Context, string, int64, *gmproto.Cursor) (*gmproto.ListMessagesResponse, error) {
					if name == "fetch error" {
						return nil, errors.New("fixture lookup failure")
					}
					if name == "cancelled" {
						return nil, context.Canceled
					}
					return &gmproto.ListMessagesResponse{Messages: []*gmproto.Message{fresh}}, nil
				}
				report := fixture.enqueueRefresh(t, "b", "$b")
				if !strings.Contains(report, "failed:") && !strings.Contains(report, "1 remain pending") {
					t.Fatalf("failure was not reported: %s", report)
				}
				if !reflect.DeepEqual(before, fixture.parts(t, ctx)) {
					t.Fatal("failed refresh changed stored pending state or media ID")
				}
				if name != "upload" && name != "send" && name != "save" && len(fixture.converted) != 0 {
					t.Fatal("validation failure started a transfer")
				}
				cancel()
				synctest.Wait()
			})
		})
	}
}

func TestRefreshMediaCommandGates(t *testing.T) {
	intent := &retryTestIntent{}
	log := zerolog.Nop()
	commandRefreshMedia.Run(&commands.Event{User: &bridgev2.User{}, Bot: intent, Log: &log, Ctx: context.Background(), Bridge: &bridgev2.Bridge{Config: &bridgeconfig.BridgeConfig{CommandPrefix: "!gm"}}})
	if len(intent.replies) != 1 || !strings.Contains(intent.replies[0], "admin") {
		t.Fatal("refresh did not reject a non-admin")
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
		commandRefreshMedia.Run(&commands.Event{User: fixture.login.User, Bot: fixture.intent, Log: &log, Ctx: ctx, Bridge: fixture.bridge, Portal: other, Args: []string{"$b"}})
		if len(fixture.intent.replies) != 1 || !strings.Contains(fixture.intent.replies[0], "not a stored message in this portal") {
			t.Fatal("refresh did not reject the wrong portal")
		}
		cancel()
		synctest.Wait()
	})
}
