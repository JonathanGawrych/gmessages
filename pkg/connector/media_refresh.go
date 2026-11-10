// SPDX-License-Identifier: AGPL-3.0-or-later

package connector

import (
	"context"
	"errors"
	"fmt"

	"github.com/rs/zerolog"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/commands"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/id"

	"go.mau.fi/mautrix-gmessages/pkg/libgm/gmproto"
)

var commandRefreshMedia = &commands.FullHandler{
	Name:          "refresh-media",
	Help:          commands.HelpMeta{Section: commands.HelpSectionChats, Description: "Refresh one pending attachment using fresh full-size media data", Args: "[original attachment event ID, or reply to that attachment]"},
	RequiresAdmin: true, RequiresPortal: true, RequiresLogin: true,
	Func: refreshMediaCommand,
}

func refreshMediaCommand(ce *commands.Event) {
	stored, login := resolveMediaCommandTarget(ce)
	if stored == nil {
		return
	}
	client := login.Client.(*GMClient)
	refresh := &mediaRefreshEvent{
		mediaRetryEvent: mediaRetryEvent{
			client: client, key: stored.Room, messageID: stored.ID,
			convert: client.convertGoogleMedia, fetch: client.Client.FetchMessages,
			reply: func(ctx context.Context, message string) {
				response := *ce
				response.Ctx = ctx
				response.Reply("%s", message)
			},
		},
		partID: stored.PartID, eventID: stored.MXID,
	}
	ce.Reply("Queueing a refresh of exactly this pending attachment from one fresh history page. No full-size request will be sent to the phone. A completion notice will verify the stored replacement.")
	result := ce.Bridge.QueueRemoteEvent(login, refresh)
	if (!result.Success || result.Ignored) && !refresh.callbackRan.Load() {
		ce.Reply("The attachment refresh could not be queued. Check the bridge logs and retry after the portal is available.")
	}
}

type mediaRefreshEvent struct {
	mediaRetryEvent
	partID          networkid.PartID
	eventID         id.EventID
	alreadyComplete bool
}

func (*mediaRefreshEvent) AddLogContext(log zerolog.Context) zerolog.Context {
	return log.Str("action", "refresh pending attachment")
}

func (refresh *mediaRefreshEvent) ConvertEdit(ctx context.Context, portal *bridgev2.Portal, intent bridgev2.MatrixAPI, existing []*database.Message) (converted *bridgev2.ConvertedEdit, returnError error) {
	defer func() {
		if returnError != nil {
			refresh.failure = returnError
			returnError = fmt.Errorf("%w: %s", bridgev2.ErrIgnoringRemoteEvent, returnError)
		}
	}()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if portal.PortalKey != refresh.key {
		return nil, errors.New("the selected attachment is not in this portal")
	}
	messageID, err := refresh.client.ParseMessageID(refresh.messageID)
	if err != nil {
		return nil, err
	}
	conversationID, err := refresh.client.ParsePortalID(portal.ID)
	if err != nil {
		return nil, err
	}
	var selected *database.Message
	for _, part := range existing {
		if part.Room != refresh.key || part.ID != refresh.messageID {
			return nil, errors.New("stored message parts do not belong to this portal and message")
		}
		if part.PartID == refresh.partID && part.MXID == refresh.eventID {
			selected = part
		}
	}
	if selected == nil || selected.HasFakeMXID() {
		return nil, errors.New("the selected original attachment is no longer stored")
	}
	metadata := selected.Metadata.(*MessageMetadata)
	if metadata.MediaPartID == "" {
		return nil, errors.New("select an attachment, not a text part")
	}
	if !metadata.MediaPending {
		refresh.alreadyComplete = true
		return &bridgev2.ConvertedEdit{}, nil
	}
	main, _, err := loadOriginalMediaMessage(ctx, portal, intent, refresh.messageID, existing)
	if err != nil {
		return nil, err
	}
	if !selected.Timestamp.Equal(main.Timestamp) || selected.SenderMXID != main.SenderMXID {
		return nil, errors.New("the selected attachment does not match the original message sender and timestamp")
	}
	message, err := refresh.client.fetchStoredMediaMessage(ctx, portal, main, messageID, conversationID, refresh.fetch)
	if err != nil {
		return nil, err
	}
	if err = validateMediaMessageContent(main, message); err != nil {
		return nil, err
	}
	var media *gmproto.MediaContent
	matches := 0
	for _, part := range message.MessageInfo {
		if part.GetActionMessageID() == metadata.MediaPartID {
			matches++
			media = part.GetMediaContent()
		}
	}
	if matches != 1 || media == nil {
		return nil, errors.New("fresh message data does not contain exactly one matching attachment part")
	}
	if media.MediaID == "" {
		return nil, errors.New("fresh attachment data has no full-size media ID; no transfer or full-size request was made")
	}
	if len(media.DecryptionKey) == 0 {
		return nil, errors.New("fresh full-size attachment data has no decryption key; no transfer was made")
	}
	return refresh.convertPendingMediaParts(ctx, portal, intent, []pendingMediaPart{{stored: selected, media: media}}, message, len(existing), "")
}

func (refresh *mediaRefreshEvent) PostHandle(ctx context.Context, portal *bridgev2.Portal) {
	if refresh.alreadyComplete {
		refresh.callbackRan.Store(true)
		refresh.reply(ctx, "The selected attachment is already complete; skipped without a history lookup or transfer.")
		return
	}
	refresh.mediaRetryEvent.PostHandle(ctx, portal)
}
