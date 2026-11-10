// SPDX-License-Identifier: AGPL-3.0-or-later

package connector

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/commands"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"go.mau.fi/mautrix-gmessages/pkg/libgm/gmproto"
)

func resolveMediaCommandTarget(ce *commands.Event) (*database.Message, *bridgev2.UserLogin) {
	target := ce.ReplyTo
	if len(ce.Args) == 1 {
		target = id.EventID(ce.Args[0])
	}
	if len(ce.Args) > 1 || !strings.HasPrefix(string(target), "$") {
		ce.Reply("Pass one original Matrix event ID, or reply to the original message with the command.")
		return nil, nil
	}
	stored, err := ce.Bridge.DB.Message.GetPartByMXID(ce.Ctx, target)
	if err != nil {
		ce.Reply("Failed to look up the stored message: %v", err)
		return nil, nil
	} else if stored == nil || stored.Room != ce.Portal.PortalKey {
		ce.Reply("The event is not a stored message in this portal. Use the original event ID in its own portal room.")
		return nil, nil
	}
	login, _, err := ce.Portal.FindPreferredLogin(ce.Ctx, ce.User, false)
	if err != nil {
		ce.Reply("An active login owned by you is required for this portal: %v", err)
		return nil, nil
	}
	if _, err = login.Client.(*GMClient).ParseMessageID(stored.ID); err != nil {
		ce.Reply("The stored message belongs to another Google Messages account.")
		return nil, nil
	}
	return stored, login
}

func loadOriginalMediaMessage(ctx context.Context, portal *bridgev2.Portal, intent bridgev2.MatrixAPI, target networkid.MessageID, existing []*database.Message) (*database.Message, *event.Event, error) {
	var main *database.Message
	for _, part := range existing {
		if part.Room != portal.PortalKey || part.ID != target || part.MXID == "" || part.HasFakeMXID() {
			return nil, nil, errors.New("stored message parts do not belong to this portal or have no original Matrix event")
		}
		if part.PartID == "" {
			main = part
		}
	}
	if main == nil {
		return nil, nil, errors.New("the stored message has no original first part in this portal")
	}
	original, err := intent.GetEvent(ctx, portal.MXID, main.MXID)
	if err != nil {
		return nil, nil, fmt.Errorf("cannot read the original Matrix event and saved raw data: %w", err)
	}
	if original.ID != main.MXID || original.RoomID != portal.MXID || original.Sender != main.SenderMXID || original.Type != event.EventMessage {
		return nil, nil, errors.New("the original Matrix event does not match the stored message")
	}
	return main, original, nil
}

func (gc *GMClient) loadMediaMessage(ctx context.Context, portal *bridgev2.Portal, intent bridgev2.MatrixAPI, target networkid.MessageID, existing []*database.Message, fetch historyPageFetcher) (*database.Message, *gmproto.Message, string, error) {
	messageID, err := gc.ParseMessageID(target)
	if err != nil {
		return nil, nil, "", err
	}
	conversationID, err := gc.ParsePortalID(portal.ID)
	if err != nil {
		return nil, nil, "", err
	}
	main, original, err := loadOriginalMediaMessage(ctx, portal, intent, target, existing)
	if err != nil {
		return nil, nil, "", err
	}
	raw, present := original.Content.Raw[rawMessageDataKey]
	encoded, isString := raw.(string)
	if present && !isString {
		return nil, nil, "", errors.New("saved raw message data is not a string; no history lookup was made")
	}
	var message *gmproto.Message
	if encoded == "" {
		message, err = gc.fetchStoredMediaMessage(ctx, portal, main, messageID, conversationID, fetch)
	} else {
		message, err = decodeSavedMessage(encoded, messageID, conversationID, main.Timestamp.UnixMicro())
	}
	if err != nil {
		return nil, nil, "", err
	}
	if err = validateMediaMessageContent(main, message); err != nil {
		return nil, nil, "", err
	}
	return main, message, encoded, nil
}

func validateMediaMessageContent(main *database.Message, message *gmproto.Message) error {
	_, textHash := getTextPart(message)
	metadata := main.Metadata.(*MessageMetadata)
	if metadata.TextHash != "" && metadata.TextHash != textHash {
		return errors.New("raw message text is stale; no attachment action was taken")
	}
	if metadata.GlobalPartCount > 0 && metadata.GlobalPartCount != len(message.MessageInfo) {
		return errors.New("raw message part count is stale; no attachment action was taken")
	}
	return nil
}
