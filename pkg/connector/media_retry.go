// SPDX-License-Identifier: AGPL-3.0-or-later

package connector

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"

	"github.com/rs/zerolog"
	"google.golang.org/protobuf/proto"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/commands"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"go.mau.fi/mautrix-gmessages/pkg/libgm/gmproto"
)

const rawMessageDataKey = "fi.mau.gmessages.raw_debug_data"

var commandRetryMedia = &commands.FullHandler{
	Name: "retry-media",
	Help: commands.HelpMeta{
		Section:     commands.HelpSectionChats,
		Description: "Retry pending attachments in one stored message, using saved media IDs",
		Args:        "[Matrix event ID, or reply to the message]",
	},
	RequiresAdmin:  true,
	RequiresPortal: true,
	RequiresLogin:  true,
	Func:           retryMediaCommand,
}

func retryMediaCommand(ce *commands.Event) {
	target := ce.ReplyTo
	if len(ce.Args) == 1 {
		target = id.EventID(ce.Args[0])
	}
	if len(ce.Args) > 1 || !strings.HasPrefix(string(target), "$") {
		ce.Reply("Usage: `$cmdprefix retry-media <Matrix event ID>`, or reply to the original message with `$cmdprefix retry-media`.")
		return
	}
	stored, err := ce.Bridge.DB.Message.GetPartByMXID(ce.Ctx, target)
	if err != nil {
		ce.Reply("Failed to look up the stored message: %v", err)
		return
	} else if stored == nil || stored.Room != ce.Portal.PortalKey {
		ce.Reply("The event is not a stored message in this portal. Use the original event ID in its own portal room.")
		return
	}
	login, _, err := ce.Portal.FindPreferredLogin(ce.Ctx, ce.User, false)
	if err != nil {
		ce.Reply("An active login owned by you is required for this portal: %v", err)
		return
	}
	client := login.Client.(*GMClient)
	if _, err = client.ParseMessageID(stored.ID); err != nil {
		ce.Reply("The stored message belongs to another Google Messages account.")
		return
	}
	retry := &mediaRetryEvent{
		client: client, key: stored.Room, messageID: stored.ID,
		convert: client.convertGoogleMedia,
		reply: func(ctx context.Context, message string) {
			response := *ce
			response.Ctx = ctx
			response.Reply("%s", message)
		},
	}
	ce.Reply("Queueing a pending-attachment retry behind earlier portal events. A completion notice will report the stored result.")
	result := ce.Bridge.QueueRemoteEvent(login, retry)
	if !result.Success || result.Ignored {
		ce.Reply("The attachment retry could not be queued. Check the bridge logs and retry after the portal is available.")
	}
}

type mediaRetryEvent struct {
	client     *GMClient
	key        networkid.PortalKey
	messageID  networkid.MessageID
	convert    func(context.Context, *bridgev2.Portal, bridgev2.MatrixAPI, *gmproto.MediaContent) (*event.MessageEventContent, string, bool, error)
	reply      func(context.Context, string)
	selected   []networkid.PartID
	converted  map[networkid.PartID]string
	thumbnails map[networkid.PartID]bool
	failure    error
}

func (retry *mediaRetryEvent) GetType() bridgev2.RemoteEventType     { return bridgev2.RemoteEventEdit }
func (retry *mediaRetryEvent) GetPortalKey() networkid.PortalKey     { return retry.key }
func (retry *mediaRetryEvent) GetTargetMessage() networkid.MessageID { return retry.messageID }
func (retry *mediaRetryEvent) GetSender() bridgev2.EventSender {
	return bridgev2.EventSender{ForceEditOrigSender: true}
}
func (retry *mediaRetryEvent) AddLogContext(log zerolog.Context) zerolog.Context {
	return log.Str("action", "retry pending media").Str("message_id", string(retry.messageID))
}

// Decoding accepts the two formats saved by the bridge: a backfilled Message,
// or an unsolicited UpdateEvents envelope. Identity checks are mandatory because
// protobuf decoding alone cannot distinguish messages with overlapping fields.
func decodeSavedMessage(encoded, messageID, conversationID string, timestamp int64) (*gmproto.Message, error) {
	if encoded == "" || len(encoded) >= 8192 {
		return nil, errors.New("saved raw message data is unavailable; this command cannot refetch history or request files from the phone")
	}
	raw, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, errors.New("saved raw message data is not valid base64")
	}
	matches := func(message *gmproto.Message) bool {
		return message.MessageID == messageID && message.ConversationID == conversationID && message.Timestamp == timestamp
	}
	message := &gmproto.Message{}
	if proto.Unmarshal(raw, message) == nil && matches(message) {
		return message, nil
	}
	updates := &gmproto.UpdateEvents{}
	if proto.Unmarshal(raw, updates) == nil {
		var found *gmproto.Message
		for _, candidate := range updates.GetMessageEvent().GetData() {
			if matches(candidate) {
				if found != nil {
					return nil, errors.New("saved raw message data contains more than one matching message")
				}
				found = candidate
			}
		}
		if found != nil {
			return found, nil
		}
	}
	return nil, errors.New("saved raw message data does not match the stored conversation, message and timestamp; no attachments were retried")
}

type pendingMediaPart struct {
	stored *database.Message
	media  *gmproto.MediaContent
}

func selectPendingMedia(existing []*database.Message, message *gmproto.Message, key networkid.PortalKey, messageID networkid.MessageID) ([]pendingMediaPart, error) {
	media := make(map[string]*gmproto.MediaContent)
	for _, part := range message.MessageInfo {
		if content := part.GetMediaContent(); content != nil {
			if part.GetActionMessageID() == "" || media[part.GetActionMessageID()] != nil {
				return nil, errors.New("saved raw message has invalid or duplicate attachment part IDs")
			}
			media[part.GetActionMessageID()] = content
		}
	}
	var selected []pendingMediaPart
	for _, stored := range existing {
		if stored.Room != key || stored.ID != messageID || stored.MXID == "" || stored.HasFakeMXID() || stored.Timestamp.UnixMicro() != message.Timestamp {
			return nil, errors.New("stored message parts do not belong to this portal or have no original Matrix event")
		}
		metadata := stored.Metadata.(*MessageMetadata)
		if !metadata.MediaPending {
			continue
		}
		content := media[metadata.MediaPartID]
		if content == nil {
			return nil, errors.New("saved raw message does not contain a pending attachment part; it may be stale")
		}
		mediaID := content.MediaID
		if mediaID == "" {
			mediaID = content.ThumbnailMediaID
		}
		if mediaID == "" || metadata.MediaID == "" {
			return nil, errors.New("a pending attachment has no saved media ID; this command does not request files from the phone")
		} else if mediaID != metadata.MediaID {
			return nil, errors.New("saved raw attachment data is stale: its media ID differs from the stored pending part")
		}
		selected = append(selected, pendingMediaPart{stored: stored, media: content})
	}
	if len(selected) == 0 {
		return nil, errors.New("this message has no pending attachments")
	}
	return selected, nil
}

func (retry *mediaRetryEvent) ConvertEdit(ctx context.Context, portal *bridgev2.Portal, intent bridgev2.MatrixAPI, existing []*database.Message) (converted *bridgev2.ConvertedEdit, returnError error) {
	defer func() {
		if returnError != nil {
			retry.failure = returnError
			returnError = fmt.Errorf("%w: %s", bridgev2.ErrIgnoringRemoteEvent, returnError)
		}
	}()
	if portal.PortalKey != retry.key || len(existing) == 0 {
		return nil, errors.New("the original message is not in the requested portal")
	}
	if !DBMessages(existing).HasPendingMedia() {
		return nil, errors.New("this message has no pending attachments")
	}
	var main *database.Message
	for _, part := range existing {
		if part.PartID == "" {
			main = part
		}
	}
	if main == nil || main.Room != retry.key {
		return nil, errors.New("the stored message has no original first part in this portal")
	}
	messageID, err := retry.client.ParseMessageID(retry.messageID)
	if err != nil {
		return nil, err
	}
	conversationID, err := retry.client.ParsePortalID(portal.ID)
	if err != nil {
		return nil, err
	}
	original, err := intent.GetEvent(ctx, portal.MXID, main.MXID)
	if err != nil {
		return nil, fmt.Errorf("cannot read the original Matrix event and saved raw data: %w", err)
	}
	if original.ID != main.MXID || original.RoomID != portal.MXID || original.Sender != main.SenderMXID || original.Type != event.EventMessage {
		return nil, errors.New("the original Matrix event does not match the stored message")
	}
	encoded, _ := original.Content.Raw[rawMessageDataKey].(string)
	message, err := decodeSavedMessage(encoded, messageID, conversationID, main.Timestamp.UnixMicro())
	if err != nil {
		return nil, err
	}
	selected, err := selectPendingMedia(existing, message, retry.key, retry.messageID)
	if err != nil {
		return nil, err
	}
	for _, part := range selected {
		if part.stored.SenderMXID != main.SenderMXID {
			return nil, errors.New("pending attachment sender differs from the original message sender")
		}
	}
	textPart, textHash := getTextPart(message)
	mainMetadata := main.Metadata.(*MessageMetadata)
	if mainMetadata.TextHash != "" && mainMetadata.TextHash != textHash {
		return nil, errors.New("saved raw message text is stale; no attachments were retried")
	}
	if mainMetadata.GlobalPartCount > 0 && mainMetadata.GlobalPartCount != len(message.MessageInfo) {
		return nil, errors.New("saved raw message part count is stale; no attachments were retried")
	}
	converted = &bridgev2.ConvertedEdit{}
	retry.converted = make(map[networkid.PartID]string)
	retry.thumbnails = make(map[networkid.PartID]bool)
	for _, part := range selected {
		retry.selected = append(retry.selected, part.stored.PartID)
	}
	for _, part := range selected {
		if err = ctx.Err(); err != nil {
			return nil, err
		}
		content, mediaID, thumbnail, mediaError := retry.convert(ctx, portal, intent, part.media)
		if mediaError != nil {
			if retry.failure == nil {
				retry.failure = fmt.Errorf("attachment transfer failed: %w", mediaError)
			}
			continue
		}
		metadata := *part.stored.Metadata.(*MessageMetadata)
		metadata.MediaPending = false
		metadata.MediaID = mediaID
		stored := *part.stored
		stored.Metadata = &metadata
		convertedPart := &bridgev2.ConvertedMessagePart{ID: stored.PartID, Type: event.EventMessage, Content: content}
		if len(existing) == 1 && textPart != nil {
			convertedPart = bridgev2.MergeCaption(textPart, convertedPart)
		}
		extra := map[string]any(nil)
		if stored.PartID == "" {
			extra = map[string]any{rawMessageDataKey: encoded}
		}
		converted.ModifiedParts = append(converted.ModifiedParts, &bridgev2.ConvertedEditPart{
			Part: &stored, Type: event.EventMessage, Content: convertedPart.Content, Extra: extra,
			TopLevelExtra: map[string]any{"com.beeper.dont_render_edited": true},
		})
		retry.converted[stored.PartID] = mediaID
		retry.thumbnails[stored.PartID] = thumbnail
		zerolog.Ctx(ctx).Debug().Bool("is_thumbnail", thumbnail).Msg("Prepared pending attachment replacement")
	}
	return converted, nil
}

func (retry *mediaRetryEvent) PostHandle(ctx context.Context, portal *bridgev2.Portal) {
	if len(retry.selected) == 0 {
		if retry.failure != nil {
			retry.reply(ctx, "Attachment retry failed: "+retry.failure.Error())
		} else {
			retry.reply(ctx, "Attachment retry did not run. Check the bridge logs for the original sender or message lookup failure.")
		}
		return
	}
	stored, err := portal.Bridge.DB.Message.GetAllPartsByID(ctx, retry.key.Receiver, retry.messageID)
	if err != nil {
		retry.reply(ctx, "Could not verify attachment retry results; check the bridge logs before retrying.")
		return
	}
	completed := 0
	thumbnails := 0
	for _, partID := range retry.selected {
		for _, part := range stored {
			metadata := part.Metadata.(*MessageMetadata)
			if part.PartID == partID && part.Room == retry.key && !metadata.MediaPending && metadata.MediaID == retry.converted[partID] {
				completed++
				if retry.thumbnails[partID] {
					thumbnails++
				}
				break
			}
		}
	}
	var message string
	if completed == len(retry.selected) {
		message = fmt.Sprintf("Retried %d pending attachment(s); Matrix edits and cleared pending flags are stored. Other attachments were untouched.", completed)
	} else {
		message = fmt.Sprintf("Retried %d attachment(s); %d remain pending. Successful parts will be skipped on the next retry.", completed, len(retry.selected)-completed)
		if retry.failure != nil {
			message += " " + retry.failure.Error()
		} else {
			message += " Check the bridge logs for Matrix send or database save errors."
		}
	}
	if thumbnails > 0 {
		message += fmt.Sprintf(" %d replacement(s) used thumbnails only; full-size media was not requested.", thumbnails)
	}
	retry.reply(ctx, message)
}
