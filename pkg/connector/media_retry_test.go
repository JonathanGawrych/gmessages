// SPDX-License-Identifier: AGPL-3.0-or-later

package connector

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/rs/zerolog"
	"go.mau.fi/util/dbutil"
	"google.golang.org/protobuf/proto"
	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/appservice"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/bridgeconfig"
	"maunium.net/go/mautrix/bridgev2/commands"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/matrix"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"
	"maunium.net/go/mautrix/sqlstatestore"

	"go.mau.fi/mautrix-gmessages/pkg/libgm/gmproto"
)

func retryFixtureMessage() *gmproto.Message {
	message := &gmproto.Message{
		MessageID: "message", ConversationID: "chat", Timestamp: 1700000000123456,
		MessageStatus: &gmproto.MessageStatus{Status: gmproto.MessageStatusType_INCOMING_COMPLETE},
	}
	for _, name := range []string{"a", "b", "c"} {
		message.MessageInfo = append(message.MessageInfo, &gmproto.MessageInfo{
			ActionMessageID: proto.String(name),
			Data:            &gmproto.MessageInfo_MediaContent{MediaContent: &gmproto.MediaContent{MediaID: name, MediaName: name + ".png"}},
		})
	}
	return message
}

type retryTestTransport func(*http.Request) (*http.Response, error)

func (transport retryTestTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	return transport(request)
}

func TestRetryMediaNativeDoublePuppetMarker(t *testing.T) {
	previous := bridgev2.EventHandlingTimeoutTicks
	defer func() { bridgev2.EventHandlingTimeoutTicks = previous }()
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		fixture := newRetryFixture(t, ctx)
		state := sqlstatestore.NewSQLStateStore(fixture.bridge.DB.Database, dbutil.ZeroLogger(zerolog.Nop()), false)
		if err := state.Upgrade(ctx); err != nil {
			t.Fatal(err)
		}
		application, err := appservice.CreateFull(appservice.CreateOpts{
			HomeserverDomain: "example.invalid", HomeserverURL: "https://example.invalid",
			Registration: &appservice.Registration{ID: "fixture", SenderLocalpart: "bot"},
		})
		if err != nil {
			t.Fatal(err)
		}
		application.DoublePuppetValue = "fixture.gmessages"
		puppet := application.Intent("@original:example.invalid")
		puppet.IsCustomPuppet = true
		if err = application.StateStore.SetMembership(ctx, fixture.portal.MXID, puppet.UserID, event.MembershipJoin); err != nil {
			t.Fatal(err)
		}
		var sent map[string]any
		puppet.Client.Client = &http.Client{Transport: retryTestTransport(func(request *http.Request) (*http.Response, error) {
			if request.Method != http.MethodPut || !strings.Contains(request.URL.Path, "/send/m.room.message/") {
				t.Fatalf("unexpected Matrix request: %s %s", request.Method, request.URL.Path)
			}
			if err := json.NewDecoder(request.Body).Decode(&sent); err != nil {
				t.Fatal(err)
			}
			return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"event_id":"$edit"}`))}, nil
		})}
		intent := &matrix.ASIntent{Matrix: puppet, Connector: &matrix.Connector{Config: &bridgeconfig.Config{}, StateStore: state}}
		content := &event.MessageEventContent{MsgType: event.MsgImage, Body: "fixture.png", URL: "mxc://example.invalid/fixture"}
		content.SetEdit("$original")
		_, err = intent.SendMessage(ctx, fixture.portal.MXID, event.EventMessage, &event.Content{Parsed: content}, &bridgev2.MatrixSendExtra{Timestamp: time.Now()})
		if err != nil {
			t.Fatal(err)
		}
		if sent[appservice.DoublePuppetKey] != application.DoublePuppetValue {
			t.Fatal("native edit lacks double-puppet echo marker")
		}
		if sent["m.relates_to"].(map[string]any)["event_id"] != "$original" {
			t.Fatal("native edit changed its target")
		}
		cancel()
		synctest.Wait()
	})
}

func encodeRetryFixture(t *testing.T, message proto.Message) string {
	t.Helper()
	raw, err := proto.Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	return base64.StdEncoding.EncodeToString(raw)
}

func TestDecodeSavedMessage(t *testing.T) {
	message := retryFixtureMessage()
	envelope := &gmproto.UpdateEvents{Event: &gmproto.UpdateEvents_MessageEvent{MessageEvent: &gmproto.MessageEvent{Data: []*gmproto.Message{message}}}}
	for _, saved := range []proto.Message{message, envelope} {
		decoded, err := decodeSavedMessage(encodeRetryFixture(t, saved), message.MessageID, message.ConversationID, message.Timestamp)
		if err != nil || !proto.Equal(decoded, message) {
			t.Fatalf("decode saved message: %v", err)
		}
	}
	for _, test := range []struct {
		name, encoded, messageID, conversationID string
		timestamp                                int64
	}{
		{"unavailable", "", message.MessageID, message.ConversationID, message.Timestamp},
		{"invalid", "!", message.MessageID, message.ConversationID, message.Timestamp},
		{"wrong message", encodeRetryFixture(t, message), "other", message.ConversationID, message.Timestamp},
		{"wrong conversation", encodeRetryFixture(t, message), message.MessageID, "other", message.Timestamp},
		{"wrong timestamp", encodeRetryFixture(t, message), message.MessageID, message.ConversationID, message.Timestamp + 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := decodeSavedMessage(test.encoded, test.messageID, test.conversationID, test.timestamp); err == nil {
				t.Fatal("accepted invalid saved message")
			}
		})
	}
}

type retryTestIntent struct {
	bridgev2.MatrixAPI
	original   *event.Event
	reads      []id.EventID
	edits      []*event.MessageEventContent
	replies    []string
	failTarget id.EventID
	readError  error
}

func (*retryTestIntent) GetMXID() id.UserID { return "@bot:example.invalid" }
func (intent *retryTestIntent) GetEvent(_ context.Context, _ id.RoomID, eventID id.EventID) (*event.Event, error) {
	intent.reads = append(intent.reads, eventID)
	return intent.original, intent.readError
}
func (intent *retryTestIntent) SendMessage(_ context.Context, _ id.RoomID, _ event.Type, content *event.Content, extra *bridgev2.MatrixSendExtra) (*mautrix.RespSendEvent, error) {
	message := content.AsMessage()
	if extra == nil {
		intent.replies = append(intent.replies, message.Body)
	} else {
		if message.RelatesTo.GetReplaceID() == intent.failTarget {
			return nil, errors.New("fixture Matrix send failure")
		}
		intent.edits = append(intent.edits, message)
	}
	return &mautrix.RespSendEvent{EventID: "$edit"}, nil
}

type retryTestMatrix struct {
	bridgev2.MatrixConnector
	intent *retryTestIntent
}

func (*retryTestMatrix) Init(*bridgev2.Bridge)                {}
func (matrix *retryTestMatrix) BotIntent() bridgev2.MatrixAPI { return matrix.intent }

type retryFixture struct {
	bridge         *bridgev2.Bridge
	portal         *bridgev2.Portal
	login          *bridgev2.UserLogin
	client         *GMClient
	intent         *retryTestIntent
	message        *gmproto.Message
	converted      []string
	convertFailure string
}

func newRetryFixture(t *testing.T, ctx context.Context) *retryFixture {
	t.Helper()
	raw, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { raw.Close() })
	raw.SetMaxOpenConns(1)
	wrapped, err := dbutil.NewWithDB(raw, "sqlite3")
	if err != nil {
		t.Fatal(err)
	}
	intent := &retryTestIntent{}
	network := &orderingTestNetwork{}
	bridge := bridgev2.NewBridge("fixture", wrapped, zerolog.Nop(), &bridgeconfig.BridgeConfig{}, &retryTestMatrix{intent: intent}, network, commands.NewProcessor)
	bridge.BackgroundCtx = ctx
	if err = bridge.DB.Upgrade(ctx); err != nil {
		t.Fatal(err)
	}
	user, err := bridge.GetUserByMXID(ctx, "@fixture:example.invalid")
	if err != nil {
		t.Fatal(err)
	}
	if err = bridge.DB.UserLogin.Insert(ctx, &database.UserLogin{ID: "fixture", UserMXID: user.MXID}); err != nil {
		t.Fatal(err)
	}
	login, err := bridge.GetExistingUserLoginByID(ctx, "fixture")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(login.BridgeState.Destroy)
	portal, err := bridge.GetPortalByKey(ctx, networkid.PortalKey{ID: "fixture.chat"})
	if err != nil {
		t.Fatal(err)
	}
	login.MarkInPortal(ctx, portal)
	portal.MXID = "!fixture:example.invalid"
	message := retryFixtureMessage()
	intent.original = &event.Event{
		ID: "$a", RoomID: portal.MXID, Sender: intent.GetMXID(), Type: event.EventMessage,
		Content: event.Content{Raw: map[string]any{rawMessageDataKey: encodeRetryFixture(t, message)}},
	}
	client := &GMClient{UserLogin: login, Meta: &UserLoginMetadata{IDPrefix: "fixture"}}
	fixture := &retryFixture{bridge: bridge, portal: portal, login: login, client: client, intent: intent, message: message}
	for index, part := range message.MessageInfo {
		partID := networkid.PartID(part.GetActionMessageID())
		if index == 0 {
			partID = ""
		}
		stored := &database.Message{
			ID: "fixture.message", PartID: partID, MXID: id.EventID("$" + part.GetActionMessageID()), Room: portal.PortalKey,
			SenderMXID: intent.GetMXID(), Timestamp: time.UnixMicro(message.Timestamp),
			Metadata: &MessageMetadata{MediaPending: index != 2, MediaID: part.GetActionMessageID(), MediaPartID: part.GetActionMessageID(), GlobalPartCount: 3, MSSSent: true, ReadReceiptSent: true},
		}
		if err = bridge.DB.Message.Insert(ctx, stored); err != nil {
			t.Fatal(err)
		}
	}
	return fixture
}

func (fixture *retryFixture) enqueue(t *testing.T) string {
	t.Helper()
	result := make(chan string, 1)
	retry := &mediaRetryEvent{
		client: fixture.client, key: fixture.portal.PortalKey, messageID: "fixture.message",
		reply: func(_ context.Context, message string) { result <- message },
		convert: func(_ context.Context, _ *bridgev2.Portal, _ bridgev2.MatrixAPI, media *gmproto.MediaContent) (*event.MessageEventContent, string, bool, error) {
			mediaID := media.MediaID
			if mediaID == "" {
				mediaID = media.ThumbnailMediaID
			}
			fixture.converted = append(fixture.converted, mediaID)
			if mediaID == fixture.convertFailure {
				return nil, mediaID, false, errors.New("fixture upload failure")
			}
			return &event.MessageEventContent{MsgType: event.MsgImage, Body: media.MediaName, URL: "mxc://example.invalid/fixture"}, mediaID, media.MediaID == "", nil
		},
	}
	if !retry.GetSender().ForceEditOrigSender {
		t.Fatal("retry must use the original Matrix sender")
	}
	queued := fixture.bridge.QueueRemoteEvent(fixture.login, retry)
	if !queued.Queued {
		t.Fatalf("enqueue retry: %+v", queued)
	}
	return <-result
}

func (fixture *retryFixture) parts(t *testing.T, ctx context.Context) []*database.Message {
	t.Helper()
	parts, err := fixture.bridge.DB.Message.GetAllPartsByID(ctx, fixture.portal.Receiver, "fixture.message")
	if err != nil {
		t.Fatal(err)
	}
	return parts
}

func TestRetryMediaNativeEditAndPartialRetry(t *testing.T) {
	previous := bridgev2.EventHandlingTimeoutTicks
	defer func() { bridgev2.EventHandlingTimeoutTicks = previous }()
	for _, failure := range []string{"upload", "send", "save"} {
		t.Run(failure, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				fixture := newRetryFixture(t, ctx)
				before := fixture.parts(t, ctx)
				switch failure {
				case "upload":
					fixture.convertFailure = "b"
				case "send":
					fixture.intent.failTarget = "$b"
				case "save":
					_, err := fixture.bridge.DB.Exec(ctx, `CREATE TRIGGER fail_retry_save BEFORE UPDATE ON message WHEN NEW.part_id='b' BEGIN SELECT RAISE(FAIL, 'fixture save failure'); END`)
					if err != nil {
						t.Fatal(err)
					}
				}
				report := fixture.enqueue(t)
				if !strings.Contains(report, "Retried 1 attachment(s); 1 remain pending") {
					t.Fatal(report)
				}
				if !reflect.DeepEqual(fixture.converted, []string{"a", "b"}) {
					t.Fatalf("converted: %v", fixture.converted)
				}
				parts := fixture.parts(t, ctx)
				for _, part := range parts {
					metadata := part.Metadata.(*MessageMetadata)
					if metadata.MediaPending != (part.PartID == "b") {
						t.Fatalf("pending flag for %s", part.PartID)
					}
					if !metadata.MSSSent || !metadata.ReadReceiptSent {
						t.Fatal("unrelated metadata lost")
					}
				}
				for _, original := range before {
					if original.PartID == "c" {
						for _, part := range parts {
							if part.PartID == "c" && !reflect.DeepEqual(original, part) {
								t.Fatal("successful attachment was modified")
							}
						}
					}
				}
				if fixture.intent.reads[0] != "$a" {
					t.Fatal("did not fetch first stored part")
				}
				fixture.convertFailure = ""
				fixture.intent.failTarget = ""
				if failure == "save" {
					if _, err := fixture.bridge.DB.Exec(ctx, `DROP TRIGGER fail_retry_save`); err != nil {
						t.Fatal(err)
					}
				}
				report = fixture.enqueue(t)
				if !strings.Contains(report, "Retried 1 pending attachment(s)") {
					t.Fatal(report)
				}
				if !reflect.DeepEqual(fixture.converted, []string{"a", "b", "b"}) {
					t.Fatalf("retry reconverted successful parts: %v", fixture.converted)
				}
				for _, edit := range fixture.intent.edits {
					if edit.RelatesTo.GetReplaceID() != "$a" && edit.RelatesTo.GetReplaceID() != "$b" {
						t.Fatal("unexpected edit target")
					}
					if edit.NewContent == nil || edit.NewContent.URL == "" {
						t.Fatal("native Matrix replacement was not produced")
					}
				}
				report = fixture.enqueue(t)
				if !strings.Contains(report, "no pending attachments") {
					t.Fatal(report)
				}
				cancel()
				synctest.Wait()
			})
		})
	}
}

func TestRetryMediaValidationAndPostHandlerFailures(t *testing.T) {
	previous := bridgev2.EventHandlingTimeoutTicks
	defer func() { bridgev2.EventHandlingTimeoutTicks = previous }()
	for _, name := range []string{"no raw", "wrong message", "wrong portal", "stale media", "no media ID", "stale text", "read failure", "different sender"} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				fixture := newRetryFixture(t, ctx)
				switch name {
				case "no raw":
					fixture.intent.original.Content.Raw = nil
				case "wrong message":
					fixture.message.MessageID = "other"
				case "wrong portal":
					fixture.message.ConversationID = "other"
				case "stale media":
					fixture.message.MessageInfo[0].GetMediaContent().MediaID = "different"
				case "no media ID":
					fixture.message.MessageInfo[0].GetMediaContent().MediaID = ""
				case "read failure":
					fixture.intent.readError = errors.New("fixture read failure")
				case "different sender":
					for _, part := range fixture.parts(t, ctx) {
						if part.PartID == "b" {
							part.SenderMXID = "@other:example.invalid"
							if err := fixture.bridge.DB.Message.Update(ctx, part); err != nil {
								t.Fatal(err)
							}
						}
					}
				case "stale text":
					parts := fixture.parts(t, ctx)
					for _, part := range parts {
						if part.PartID == "" {
							part.Metadata.(*MessageMetadata).TextHash = "different"
							if err := fixture.bridge.DB.Message.Update(ctx, part); err != nil {
								t.Fatal(err)
							}
						}
					}
				}
				if name != "no raw" {
					fixture.intent.original.Content.Raw[rawMessageDataKey] = encodeRetryFixture(t, fixture.message)
				}
				report := fixture.enqueue(t)
				if !strings.Contains(report, "Attachment retry failed") {
					t.Fatal(report)
				}
				if len(fixture.converted) != 0 || len(fixture.intent.edits) != 0 {
					t.Fatal("invalid retry transferred or edited an attachment")
				}
				for _, part := range fixture.parts(t, ctx) {
					if part.Metadata.(*MessageMetadata).MediaPending != (part.PartID != "c") {
						t.Fatal("failure changed pending flags")
					}
				}
				cancel()
				synctest.Wait()
			})
		})
	}
}

func TestRetryMediaRawOnTextPartAndThumbnailReport(t *testing.T) {
	previous := bridgev2.EventHandlingTimeoutTicks
	defer func() { bridgev2.EventHandlingTimeoutTicks = previous }()
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		fixture := newRetryFixture(t, ctx)
		for _, part := range fixture.parts(t, ctx) {
			if part.PartID == "" {
				part.PartID = "a"
				if err := fixture.bridge.DB.Message.Update(ctx, part); err != nil {
					t.Fatal(err)
				}
			}
		}
		fixture.message.MessageInfo[0].GetMediaContent().MediaID = ""
		fixture.message.MessageInfo[0].GetMediaContent().ThumbnailMediaID = "a"
		fixture.message.MessageInfo = append(fixture.message.MessageInfo, &gmproto.MessageInfo{
			Data: &gmproto.MessageInfo_MessageContent{MessageContent: &gmproto.MessageContent{Content: "Separate text"}},
		})
		_, textHash := getTextPart(fixture.message)
		text := &database.Message{
			ID: "fixture.message", PartID: "", MXID: "$text", Room: fixture.portal.PortalKey,
			SenderMXID: fixture.intent.GetMXID(), Timestamp: time.UnixMicro(fixture.message.Timestamp),
			Metadata: &MessageMetadata{TextHash: textHash, GlobalPartCount: 4},
		}
		if err := fixture.bridge.DB.Message.Insert(ctx, text); err != nil {
			t.Fatal(err)
		}
		fixture.intent.original.ID = "$text"
		fixture.intent.original.Content.Raw[rawMessageDataKey] = encodeRetryFixture(t, fixture.message)
		report := fixture.enqueue(t)
		if !strings.Contains(report, "Retried 2 pending") || !strings.Contains(report, "1 replacement(s) used thumbnails only") {
			t.Fatal(report)
		}
		if len(fixture.intent.reads) != 1 || fixture.intent.reads[0] != "$text" {
			t.Fatalf("read wrong raw source: %v", fixture.intent.reads)
		}
		for _, edit := range fixture.intent.edits {
			if edit.RelatesTo.GetReplaceID() == "$text" || edit.NewContent.Body == "Separate text" {
				t.Fatal("separate text was changed or merged")
			}
		}
		cancel()
		synctest.Wait()
	})
}

func TestRetryMediaAdminGate(t *testing.T) {
	intent := &retryTestIntent{}
	user := &bridgev2.User{Permissions: bridgeconfig.Permissions{}}
	log := zerolog.Nop()
	ce := &commands.Event{User: user, Bot: intent, Log: &log, Ctx: context.Background(), Bridge: &bridgev2.Bridge{Config: &bridgeconfig.BridgeConfig{CommandPrefix: "!gm"}}}
	commandRetryMedia.Run(ce)
	if len(intent.replies) != 1 || !strings.Contains(intent.replies[0], "administrators") {
		t.Fatalf("non-admin response: %v", intent.replies)
	}
}

func TestRetryMediaPreservesCaptionAndPortalOrder(t *testing.T) {
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
				if err := fixture.bridge.DB.Message.Update(ctx, part); err != nil {
					t.Fatal(err)
				}
			}
		}
		fixture.message.MessageInfo = append(fixture.message.MessageInfo[:1], &gmproto.MessageInfo{
			Data: &gmproto.MessageInfo_MessageContent{MessageContent: &gmproto.MessageContent{Content: "A saved caption"}},
		})
		fixture.intent.original.Content.Raw[rawMessageDataKey] = encodeRetryFixture(t, fixture.message)
		blocked := make(chan struct{})
		release := make(chan struct{})
		fixture.bridge.QueueRemoteEvent(fixture.login, orderingTestEvent{key: fixture.portal.PortalKey, handle: func() { close(blocked); <-release }})
		<-blocked
		finished := make(chan string, 1)
		go func() { finished <- fixture.enqueue(t) }()
		synctest.Wait()
		if len(fixture.converted) != 0 {
			t.Fatal("retry overtook earlier portal event")
		}
		close(release)
		if report := <-finished; !strings.Contains(report, "Retried 1 pending") {
			t.Fatal(report)
		}
		content := fixture.intent.edits[0].NewContent
		if content.Body != "A saved caption" || content.FileName != "a.png" {
			t.Fatalf("caption changed: %+v", content)
		}
		part := fixture.parts(t, ctx)[0]
		if part.PartID != "" || part.MXID != "$a" || part.Timestamp.UnixMicro() != fixture.message.Timestamp || part.SenderMXID != fixture.intent.GetMXID() {
			t.Fatal("stored message identity changed")
		}
		cancel()
		synctest.Wait()
	})
}

func TestRetryMediaCommandRejectsWrongPortal(t *testing.T) {
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
		log := zerolog.Nop()
		commandRetryMedia.Run(&commands.Event{
			User: fixture.login.User, Bot: fixture.intent, Log: &log, Ctx: ctx, Bridge: fixture.bridge,
			Portal: other, Args: []string{"$a"},
		})
		if len(fixture.intent.replies) != 1 || !strings.Contains(fixture.intent.replies[0], "not a stored message in this portal") {
			t.Fatalf("wrong-portal response: %v", fixture.intent.replies)
		}
		if len(fixture.converted) != 0 {
			t.Fatal("wrong-portal command transferred media")
		}
		cancel()
		synctest.Wait()
	})
}
