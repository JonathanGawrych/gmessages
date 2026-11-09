// SPDX-License-Identifier: AGPL-3.0-or-later

package connector

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/rs/zerolog"
	"google.golang.org/protobuf/proto"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"

	"go.mau.fi/mautrix-gmessages/pkg/libgm"
	"go.mau.fi/mautrix-gmessages/pkg/libgm/gmproto"
)

type preparedForwardHistory struct {
	response *bridgev2.FetchMessagesResponse
	complete func(context.Context) error
}

func saveForwardHistory(ctx context.Context, portal *bridgev2.Portal, state *ForwardHistoryMetadata) error {
	metadata := portal.Metadata.(*PortalMetadata)
	previous := metadata.ForwardHistory
	metadata.ForwardHistory = state
	if err := portal.Save(ctx); err != nil {
		metadata.ForwardHistory = previous
		return fmt.Errorf("save forward history position: %w", err)
	}
	return nil
}

func (gc *GMClient) bootstrapForwardHistory(ctx context.Context) error {
	portals, err := gc.Main.br.DB.UserPortal.GetAllForLogin(ctx, gc.UserLogin.UserLogin)
	if err != nil {
		return fmt.Errorf("get portals for forward history bootstrap: %w", err)
	}
	for _, userPortal := range portals {
		portal, err := gc.Main.br.GetExistingPortalByKey(ctx, userPortal.Portal)
		if err != nil {
			return fmt.Errorf("get portal for forward history bootstrap: %w", err)
		}
		if portal == nil {
			return fmt.Errorf("portal for forward history bootstrap no longer exists")
		}
		if portal.Metadata.(*PortalMetadata).ForwardHistory != nil || portal.MXID == "" {
			continue
		}
		latest, err := gc.Main.br.DB.Message.GetLastPartAtOrBeforeTime(ctx, portal.PortalKey, time.Now().Add(10*time.Second))
		if err != nil {
			return fmt.Errorf("get persisted message for forward history bootstrap: %w", err)
		}
		if latest == nil {
			continue
		}
		messageID, err := gc.ParseMessageID(latest.ID)
		if err != nil {
			return fmt.Errorf("parse forward history bootstrap message ID: %w", err)
		}
		position := &HistoryPosition{Timestamp: latest.Timestamp.UnixMicro(), MessageIDs: []string{messageID}}
		if err = saveForwardHistory(ctx, portal, &ForwardHistoryMetadata{
			Completed: position, BootstrapAnchor: position, CatchupPending: true,
		}); err != nil {
			return err
		}
		portal.Log.Info().Time("bootstrap_message_ts", latest.Timestamp).
			Msg("Saved forward history bootstrap before network connection; earlier history coverage is unverified")
	}
	return nil
}

func (gc *GMClient) prepareForwardHistory(ctx context.Context, portal *bridgev2.Portal, conversationID string, initialLimit int) (*preparedForwardHistory, error) {
	state := portal.Metadata.(*PortalMetadata).ForwardHistory
	if state == nil {
		state = &ForwardHistoryMetadata{Initial: &InitialHistoryPending{Limit: initialLimit}}
		if err := saveForwardHistory(ctx, portal, state); err != nil {
			return nil, err
		}
		zerolog.Ctx(ctx).Info().Int("initial_limit", initialLimit).
			Msg("Starting initial forward history window; earlier history coverage is unverified")
	}
	pager := newHistoryPager(gc.Client.FetchMessages, conversationID)
	var messages []*gmproto.Message
	var err error
	if state.Initial != nil {
		pending := *state.Initial
		messages, err = collectInitialHistory(ctx, pager, &pending, func(timestamp int64) error {
			pending.UpperTimestamp = &timestamp
			next := *state
			next.Initial = &pending
			if err := saveForwardHistory(ctx, portal, &next); err != nil {
				return err
			}
			state = &next
			return nil
		})
	} else {
		if !state.CatchupPending {
			next := *state
			next.CatchupPending = true
			if err = saveForwardHistory(ctx, portal, &next); err != nil {
				return nil, err
			}
			state = &next
		}
		messages, err = collectCatchupHistory(ctx, pager, state.Completed, gc.Main.br.Config.Backfill.MaxCatchupMessages)
	}
	if err != nil {
		return nil, err
	}
	progress := zerolog.Ctx(ctx).Info().Int("pages", pager.pages).Int("raw_messages", len(messages))
	if len(messages) > 0 {
		progress.Time("oldest_message_ts", time.UnixMicro(messages[0].Timestamp)).
			Time("newest_message_ts", time.UnixMicro(messages[len(messages)-1].Timestamp))
	}
	progress.Msg("Completed forward history raw fetch; starting message and media conversion")
	position := advanceHistoryPosition(state.Completed, messages)
	converted, err := gc.convertForwardHistory(ctx, portal, messages)
	if err != nil {
		return nil, err
	}
	lookup := func(messageID networkid.MessageID) ([]*database.Message, error) {
		return gc.Main.br.DB.Message.GetAllPartsByID(ctx, portal.Receiver, messageID)
	}
	filtered, expected, err := filterStoredHistoryParts(converted, lookup)
	if err != nil {
		return nil, err
	}
	response := &bridgev2.FetchMessagesResponse{
		Messages:         filtered,
		Forward:          true,
		HasMore:          !pager.done,
		ApproxTotalCount: int(pager.totalMessages),
	}
	if len(messages) > 0 {
		latest := messages[len(messages)-1]
		gc.conversationMetaLock.Lock()
		metadata := gc.conversationMeta[conversationID]
		if metadata != nil {
			response.MarkRead = !metadata.unread || !metadata.readUpToTS.Before(time.UnixMicro(latest.Timestamp)) || metadata.readUpTo == latest.MessageID
		}
		gc.conversationMetaLock.Unlock()
	}
	zerolog.Ctx(ctx).Info().Int("raw_messages", len(messages)).Int("messages_to_send", len(filtered)).
		Msg("Prepared forward history after fetching all pages")
	return &preparedForwardHistory{
		response: response,
		complete: func(completionContext context.Context) error {
			next := &ForwardHistoryMetadata{Completed: position, BootstrapAnchor: state.BootstrapAnchor}
			err := completeForwardHistory(completionContext, expected, func(messageID networkid.MessageID) ([]*database.Message, error) {
				return gc.Main.br.DB.Message.GetAllPartsByID(completionContext, portal.Receiver, messageID)
			}, func(state *ForwardHistoryMetadata) error {
				return saveForwardHistory(completionContext, portal, state)
			}, next)
			if err != nil {
				return err
			}
			zerolog.Ctx(completionContext).Info().Int("verified_messages", len(expected)).
				Time("completed_message_ts", time.UnixMicro(position.Timestamp)).
				Msg("Completed forward history and verified stored message parts")
			return nil
		},
	}, nil
}

func (gc *GMClient) convertForwardHistory(ctx context.Context, portal *bridgev2.Portal, messages []*gmproto.Message) ([]*bridgev2.BackfillMessage, error) {
	converted := make([]*bridgev2.BackfillMessage, 0, len(messages))
	for index, message := range messages {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		messageTimestamp := time.UnixMicro(message.Timestamp)
		log := zerolog.Ctx(ctx).With().Str("message_id", message.MessageID).Time("message_ts", messageTimestamp).Logger()
		messageContext := log.WithContext(ctx)
		sender := gc.getEventSenderFromMessage(message)
		intent, ok := portal.GetIntentFor(messageContext, sender, gc.UserLogin, bridgev2.RemoteEventBackfill)
		if !ok {
			return nil, fmt.Errorf("get sender intent for forward history message %s", message.MessageID)
		}
		rawData, err := proto.Marshal(message)
		if err != nil {
			return nil, fmt.Errorf("marshal forward history message: %w", err)
		}
		converted = append(converted, &bridgev2.BackfillMessage{
			ConvertedMessage: gc.ConvertGoogleMessage(messageContext, portal, intent, &libgm.WrappedMessage{
				Message: message,
				Data:    rawData,
			}, true),
			Sender:      sender,
			ID:          gc.MakeMessageID(message.MessageID),
			TxnID:       networkid.TransactionID(message.TmpID),
			Timestamp:   messageTimestamp,
			StreamOrder: message.Timestamp,
			Reactions:   (&ReactionSyncEvent{Message: message, g: gc}).GetReactions().ToBackfill(),
		})
		if (index+1)%100 == 0 || index+1 == len(messages) {
			zerolog.Ctx(ctx).Info().Int("converted_messages", index+1).Int("total_messages", len(messages)).
				Time("message_ts", messageTimestamp).Msg("Converting forward history messages and media")
		}
	}
	return converted, nil
}

type historyPartLookup func(networkid.MessageID) ([]*database.Message, error)
type expectedHistoryParts map[networkid.MessageID][]networkid.PartID

func completeForwardHistory(ctx context.Context, expected expectedHistoryParts, lookup historyPartLookup, save func(*ForwardHistoryMetadata) error, next *ForwardHistoryMetadata) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := verifyStoredHistoryParts(expected, lookup); err != nil {
		return err
	}
	return save(next)
}

func filterStoredHistoryParts(messages []*bridgev2.BackfillMessage, lookup historyPartLookup) ([]*bridgev2.BackfillMessage, expectedHistoryParts, error) {
	filtered := make([]*bridgev2.BackfillMessage, 0, len(messages))
	expected := make(expectedHistoryParts)
	for _, message := range messages {
		stored, err := lookup(message.ID)
		if err != nil {
			return nil, nil, fmt.Errorf("look up forward history message parts: %w", err)
		}
		parts := make([]*bridgev2.ConvertedMessagePart, 0, len(message.Parts))
		for _, part := range message.Parts {
			expected[message.ID] = append(expected[message.ID], part.ID)
			if !slices.ContainsFunc(stored, func(row *database.Message) bool { return row.PartID == part.ID }) {
				parts = append(parts, part)
			}
		}
		if len(parts) > 0 {
			copyMessage := *message
			copyConverted := *message.ConvertedMessage
			copyConverted.Parts = parts
			copyMessage.ConvertedMessage = &copyConverted
			filtered = append(filtered, &copyMessage)
		}
	}
	return filtered, expected, nil
}

func verifyStoredHistoryParts(expected expectedHistoryParts, lookup historyPartLookup) error {
	for messageID, parts := range expected {
		stored, err := lookup(messageID)
		if err != nil {
			return fmt.Errorf("verify forward history message parts: %w", err)
		}
		for _, partID := range parts {
			if !slices.ContainsFunc(stored, func(row *database.Message) bool { return row.PartID == partID && row.MXID != "" }) {
				return fmt.Errorf("forward history message %s part %s was not stored; completed history position was not advanced", messageID, partID)
			}
		}
	}
	return nil
}

func needsForwardHistory(state *ForwardHistoryMetadata, conversation *gmproto.Conversation) bool {
	if state == nil || state.Initial != nil || state.CatchupPending {
		return true
	}
	if conversation.LastMessageTimestamp == 0 && conversation.LatestMessageID == "" {
		return false
	}
	return conversation.LastMessageTimestamp > state.Completed.Timestamp ||
		(conversation.LastMessageTimestamp == state.Completed.Timestamp && !slices.Contains(state.Completed.MessageIDs, conversation.LatestMessageID))
}

func (gc *GMClient) syncForwardHistory(ctx context.Context, portal *bridgev2.Portal, conversation *gmproto.Conversation) error {
	state := portal.Metadata.(*PortalMetadata).ForwardHistory
	if !needsForwardHistory(state, conversation) {
		return nil
	}
	config := gc.Main.br.Config.Backfill
	if state == nil || state.Initial != nil {
		if config.MaxInitialMessages <= 0 {
			return nil
		}
	} else if config.MaxCatchupMessages <= 0 {
		return nil
	}
	prepared, err := gc.prepareForwardHistory(ctx, portal, conversation.ConversationID, config.MaxInitialMessages)
	if err != nil {
		return err
	}
	if len(prepared.response.Messages) > 0 {
		// Forward framework cutoffs use the latest live message, which cannot certify gap coverage.
		// The portal event loop serializes this send with live messages; stored parts are deduplicated above.
		err = portal.Internal().SendBackfill(ctx, gc.UserLogin, prepared.response.Messages, true, prepared.response.MarkRead, false, nil)
		if err != nil {
			return err
		}
	}
	return prepared.complete(ctx)
}
