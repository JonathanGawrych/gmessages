// SPDX-License-Identifier: AGPL-3.0-or-later

package connector

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/rs/zerolog"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"

	"go.mau.fi/mautrix-gmessages/pkg/libgm/gmproto"
)

const mediaLookupPageSize int64 = 100

func (gc *GMClient) fetchStoredMediaMessage(ctx context.Context, portal *bridgev2.Portal, stored *database.Message, messageID, conversationID string, fetch historyPageFetcher) (*gmproto.Message, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// Google cursors have millisecond precision. An actual newer message avoids
	// excluding the selected message or guessing the order of equal timestamps.
	endOfMillisecond := stored.Timestamp.Truncate(time.Millisecond).Add(time.Millisecond - time.Nanosecond)
	anchor, err := portal.Bridge.DB.Message.GetFirstNonFakePartAfterTime(ctx, portal.PortalKey, endOfMillisecond)
	if err != nil {
		return nil, fmt.Errorf("cannot find a stored media lookup anchor: %w", err)
	}
	var cursor *gmproto.Cursor
	if anchor != nil {
		anchorID, parseError := gc.ParseMessageID(anchor.ID)
		if parseError != nil {
			return nil, fmt.Errorf("media lookup anchor belongs to another account: %w", parseError)
		}
		cursor = &gmproto.Cursor{LastItemID: anchorID, LastItemTimestamp: anchor.Timestamp.UnixMilli()}
	}
	response, err := fetch(ctx, conversationID, mediaLookupPageSize, cursor)
	if err != nil {
		return nil, fmt.Errorf("saved raw message data is unavailable and the one-page lookup failed: %w", err)
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	if response == nil {
		return nil, errors.New("media lookup returned no response; no attachment action was taken")
	}
	if len(response.Messages) > int(mediaLookupPageSize) {
		return nil, errors.New("media lookup exceeded its 100-message bound; no attachment action was taken")
	}
	var found *gmproto.Message
	for _, candidate := range response.Messages {
		if candidate.GetMessageID() != messageID {
			continue
		}
		if candidate.ConversationID != conversationID || candidate.Timestamp != stored.Timestamp.UnixMicro() {
			return nil, errors.New("fetched message does not match the stored conversation and timestamp; no attachment action was taken")
		}
		if found != nil {
			return nil, errors.New("media lookup returned duplicate matching message IDs; no attachment action was taken")
		}
		found = candidate
	}
	if found == nil {
		return nil, errors.New("saved raw message data is unavailable and the selected message was not in the single nearby 100-message page; no further pages were fetched and no attachment action was taken")
	}
	zerolog.Ctx(ctx).Info().Int("message_count", len(response.Messages)).Bool("has_anchor", anchor != nil).
		Msg("Found stored media message in one history page after saved raw data was unavailable")
	return found, nil
}
