// SPDX-License-Identifier: AGPL-3.0-or-later

package connector

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/rs/zerolog"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/commands"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/id"

	"go.mau.fi/mautrix-gmessages/pkg/libgm/gmproto"
)

var commandInspectMedia = &commands.FullHandler{
	Name:          "inspect-media",
	Help:          commands.HelpMeta{Section: commands.HelpSectionChats, Description: "Read fresh field-presence diagnostics for one stored attachment", Args: "[original attachment event ID, or reply to that attachment]"},
	RequiresAdmin: true, RequiresPortal: true, RequiresLogin: true,
	Func: inspectMediaCommand,
}

func inspectMediaCommand(ce *commands.Event) {
	stored, login := resolveMediaCommandTarget(ce)
	if stored == nil {
		return
	}
	client := login.Client.(*GMClient)
	inspection := &mediaInspectionEvent{
		client: client, key: stored.Room, messageID: stored.ID, partID: stored.PartID, eventID: stored.MXID,
		fetch: client.Client.FetchMessages,
		reply: func(ctx context.Context, message string) {
			response := *ce
			response.Ctx = ctx
			response.Reply("%s", message)
		},
	}
	result := ce.Bridge.QueueRemoteEvent(login, inspection)
	if (!result.Success || result.Ignored) && !inspection.callbackRan.Load() {
		ce.Reply("queued=false lookup_succeeded=false")
	}
}

type mediaInspectionEvent struct {
	client      *GMClient
	key         networkid.PortalKey
	messageID   networkid.MessageID
	partID      networkid.PartID
	eventID     id.EventID
	fetch       historyPageFetcher
	reply       func(context.Context, string)
	callbackRan atomic.Bool
}

func (*mediaInspectionEvent) GetType() bridgev2.RemoteEventType            { return bridgev2.RemoteEventUnknown }
func (inspection *mediaInspectionEvent) GetPortalKey() networkid.PortalKey { return inspection.key }
func (*mediaInspectionEvent) GetSender() bridgev2.EventSender              { return bridgev2.EventSender{} }
func (*mediaInspectionEvent) AddLogContext(log zerolog.Context) zerolog.Context {
	return log.Str("action", "inspect attachment fields")
}

func (inspection *mediaInspectionEvent) PreHandle(ctx context.Context, portal *bridgev2.Portal) {
	inspection.callbackRan.Store(true)
	report := inspection.inspectStoredMedia(ctx, portal)
	inspection.reply(ctx, report)
}

func (inspection *mediaInspectionEvent) inspectStoredMedia(ctx context.Context, portal *bridgev2.Portal) string {
	const failure = "lookup_succeeded=false"
	if ctx.Err() != nil || portal.PortalKey != inspection.key {
		return failure
	}
	messageID, err := inspection.client.ParseMessageID(inspection.messageID)
	if err != nil {
		return failure
	}
	conversationID, err := inspection.client.ParsePortalID(portal.ID)
	if err != nil {
		return failure
	}
	existing, err := portal.Bridge.DB.Message.GetAllPartsByID(ctx, inspection.key.Receiver, inspection.messageID)
	if err != nil {
		return failure
	}
	var selected *database.Message
	for _, part := range existing {
		if part.PartID == inspection.partID && part.MXID == inspection.eventID {
			selected = part
		}
	}
	if selected == nil || selected.Metadata.(*MessageMetadata).MediaPartID == "" {
		return failure
	}
	main, _, err := loadOriginalMediaMessage(ctx, portal, portal.Bridge.Bot, inspection.messageID, existing)
	if err != nil || !selected.Timestamp.Equal(main.Timestamp) || selected.SenderMXID != main.SenderMXID {
		return failure
	}
	message, err := inspection.client.fetchStoredMediaMessage(ctx, portal, main, messageID, conversationID, inspection.fetch)
	if err != nil || validateMediaMessageContent(main, message) != nil {
		return failure
	}
	metadata := selected.Metadata.(*MessageMetadata)
	var media *gmproto.MediaContent
	matches := 0
	for _, part := range message.MessageInfo {
		if part.GetActionMessageID() == metadata.MediaPartID {
			matches++
			media = part.GetMediaContent()
		}
	}
	if matches != 1 || media == nil {
		return failure
	}
	return fmt.Sprintf("lookup_succeeded=true stored_pending=%t stored_has_media_id=%t has_full_id=%t has_full_key=%t has_thumbnail_id=%t has_thumbnail_key=%t inline_bytes=%d stored_parts=%d fetched_parts=%d",
		metadata.MediaPending, metadata.MediaID != "", media.MediaID != "", len(media.DecryptionKey) > 0,
		media.ThumbnailMediaID != "", len(media.ThumbnailDecryptionKey) > 0, len(media.MediaData), len(existing), len(message.MessageInfo))
}
