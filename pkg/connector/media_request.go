// SPDX-License-Identifier: AGPL-3.0-or-later

package connector

import (
	"context"
	"errors"
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

var commandRequestMedia = &commands.FullHandler{
	Name:          "request-media",
	Help:          commands.HelpMeta{Section: commands.HelpSectionChats, Description: "Request one pending attachment without a saved media ID from the paired phone", Args: "[original attachment event ID, or reply to that attachment]"},
	RequiresAdmin: true, RequiresPortal: true, RequiresLogin: true,
	Func: requestMediaCommand,
}

func requestMediaCommand(ce *commands.Event) {
	stored, login := resolveMediaCommandTarget(ce)
	if stored == nil {
		return
	}
	client := login.Client.(*GMClient)
	request := &mediaRequestEvent{
		client: client, key: stored.Room, messageID: stored.ID, partID: stored.PartID, eventID: stored.MXID,
		request: client.Client.GetFullSizeImage,
		reply: func(ctx context.Context, message string) {
			response := *ce
			response.Ctx = ctx
			response.Reply("%s", message)
		},
	}
	ce.Reply("Queueing a request for exactly this pending attachment. Acknowledgment does not confirm media recovery.")
	result := ce.Bridge.QueueRemoteEvent(login, request)
	if (!result.Success || result.Ignored) && !request.callbackRan.Load() {
		ce.Reply("The attachment request could not be queued. Check the bridge logs and retry after the portal is available.")
	}
}

type mediaRequestEvent struct {
	client      *GMClient
	key         networkid.PortalKey
	messageID   networkid.MessageID
	partID      networkid.PartID
	eventID     id.EventID
	request     func(context.Context, string, string) (*gmproto.GetFullSizeImageResponse, error)
	reply       func(context.Context, string)
	callbackRan atomic.Bool
}

// The request uses the portal queue without invoking a Matrix edit handler.
func (*mediaRequestEvent) GetType() bridgev2.RemoteEventType         { return bridgev2.RemoteEventUnknown }
func (request *mediaRequestEvent) GetPortalKey() networkid.PortalKey { return request.key }
func (*mediaRequestEvent) GetSender() bridgev2.EventSender           { return bridgev2.EventSender{} }
func (request *mediaRequestEvent) AddLogContext(log zerolog.Context) zerolog.Context {
	return log.Str("action", "request pending media").Str("message_id", string(request.messageID)).Str("part_id", string(request.partID))
}

func (request *mediaRequestEvent) PreHandle(ctx context.Context, portal *bridgev2.Portal) {
	request.callbackRan.Store(true)
	if err := request.requestStoredMedia(ctx, portal); err != nil {
		request.reply(ctx, "Attachment request failed: "+err.Error())
	} else {
		request.reply(ctx, "The paired phone acknowledged the request for one attachment. This does not confirm media availability or a saved replacement. The pending flag is unchanged by this command; recovery requires a later Google update and successful native Matrix edit.")
	}
}

func (request *mediaRequestEvent) requestStoredMedia(ctx context.Context, portal *bridgev2.Portal) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if portal.PortalKey != request.key {
		return errors.New("the selected attachment is not in this portal")
	}
	existing, err := portal.Bridge.DB.Message.GetAllPartsByID(ctx, request.key.Receiver, request.messageID)
	if err != nil {
		return fmt.Errorf("cannot load the stored message: %w", err)
	}
	var selected *database.Message
	for _, part := range existing {
		if part.PartID == request.partID && part.MXID == request.eventID {
			selected = part
		}
	}
	if selected == nil {
		return errors.New("the selected original attachment is no longer stored")
	}
	metadata := selected.Metadata.(*MessageMetadata)
	if !metadata.MediaPending || metadata.MediaPartID == "" {
		return errors.New("select a pending attachment notice, not a successful attachment or a text part")
	}
	if metadata.MediaID != "" {
		return errors.New("this attachment already has a saved media ID; use retry-media instead")
	}
	main, message, _, err := request.client.loadSavedMediaMessage(ctx, portal, portal.Bridge.Bot, request.messageID, existing)
	if err != nil {
		return err
	}
	if !selected.Timestamp.Equal(main.Timestamp) || selected.SenderMXID != main.SenderMXID {
		return errors.New("the selected attachment does not match the original message")
	}
	var media *gmproto.MediaContent
	matched := false
	for _, part := range message.MessageInfo {
		if part.GetActionMessageID() == metadata.MediaPartID {
			if matched {
				return errors.New("saved raw data contains duplicate attachment part IDs")
			}
			matched = true
			media = part.GetMediaContent()
		}
	}
	if media == nil {
		return errors.New("saved raw data does not contain the selected attachment; it may be stale")
	}
	if media.MediaID != "" || media.ThumbnailMediaID != "" {
		return errors.New("saved raw data already has a media ID but the pending record does not match; no phone request was made")
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	_, err = request.request(ctx, message.MessageID, metadata.MediaPartID)
	if err != nil {
		return fmt.Errorf("the full-size media request was not acknowledged: %w", err)
	}
	return nil
}
