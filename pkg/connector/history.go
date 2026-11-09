// SPDX-License-Identifier: AGPL-3.0-or-later

package connector

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/rs/zerolog"

	"go.mau.fi/mautrix-gmessages/pkg/libgm/gmproto"
)

const historyPageSize int64 = 100

var ErrCatchupLimit = errors.New("message history gap exceeds max_catchup_messages; completed history position was not advanced")

// HistoryPosition records a fully processed timestamp, including distinct IDs at that timestamp.
type HistoryPosition struct {
	Timestamp  int64    `json:"timestamp_us"`
	MessageIDs []string `json:"message_ids"`
}

type InitialHistoryPending struct {
	Limit          int    `json:"limit"`
	UpperTimestamp *int64 `json:"upper_timestamp_us,omitempty"`
}

// ForwardHistoryMetadata describes verified forward coverage, not the latest live message.
// History older than the configured initial window is not certified by this state.
type ForwardHistoryMetadata struct {
	Completed       *HistoryPosition       `json:"completed,omitempty"`
	BootstrapAnchor *HistoryPosition       `json:"bootstrap_anchor,omitempty"`
	Initial         *InitialHistoryPending `json:"initial,omitempty"`
	CatchupPending  bool                   `json:"catchup_pending,omitempty"`
}

type historyPageFetcher func(context.Context, string, int64, *gmproto.Cursor) (*gmproto.ListMessagesResponse, error)

type historyPager struct {
	fetch          historyPageFetcher
	conversationID string
	cursor         *gmproto.Cursor
	seenMessages   map[string]struct{}
	seenCursors    map[historyCursor]struct{}
	pages          int
	totalMessages  int64
	done           bool
}

type historyCursor struct {
	id        string
	timestamp int64
}

func newHistoryPager(fetch historyPageFetcher, conversationID string) *historyPager {
	return &historyPager{
		fetch: fetch, conversationID: conversationID,
		seenMessages: make(map[string]struct{}),
		seenCursors:  make(map[historyCursor]struct{}),
	}
}

func (pager *historyPager) next(ctx context.Context) ([]*gmproto.Message, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	pager.pages++
	response, err := pager.fetch(ctx, pager.conversationID, historyPageSize, pager.cursor)
	if err != nil {
		return nil, fmt.Errorf("fetch history page %d: %w", pager.pages, err)
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	pager.totalMessages = max(pager.totalMessages, response.TotalMessages)
	if len(response.Messages) == 0 {
		pager.done = true
		return nil, nil
	}
	oldest := response.Messages[0]
	newest := oldest.Timestamp
	messages := make([]*gmproto.Message, 0, len(response.Messages))
	for _, message := range response.Messages {
		if message.MessageID == "" {
			return nil, fmt.Errorf("history page %d contains an empty message ID", pager.pages)
		}
		if message.Timestamp <= oldest.Timestamp {
			oldest = message
		}
		newest = max(newest, message.Timestamp)
		if _, seen := pager.seenMessages[message.MessageID]; !seen {
			pager.seenMessages[message.MessageID] = struct{}{}
			messages = append(messages, message)
		}
	}
	if len(messages) == 0 {
		return nil, fmt.Errorf("history page %d made no message ID progress", pager.pages)
	}
	nextCursor := response.Cursor
	if nextCursor == nil {
		// Google message timestamps are microseconds; pagination cursors use milliseconds.
		nextCursor = &gmproto.Cursor{
			LastItemID:        oldest.MessageID,
			LastItemTimestamp: time.UnixMicro(oldest.Timestamp).UnixMilli(),
		}
	}
	if nextCursor.LastItemID == "" {
		return nil, fmt.Errorf("history page %d contains an empty cursor ID", pager.pages)
	}
	key := historyCursor{nextCursor.LastItemID, nextCursor.LastItemTimestamp}
	if _, seen := pager.seenCursors[key]; seen {
		return nil, fmt.Errorf("history page %d repeated a cursor", pager.pages)
	}
	if pager.cursor != nil && nextCursor.LastItemTimestamp > pager.cursor.LastItemTimestamp {
		return nil, fmt.Errorf("history page %d moved the cursor to a newer timestamp", pager.pages)
	}
	// A different ID at the same millisecond can advance through equal timestamps.
	pager.seenCursors[key] = struct{}{}
	pager.cursor = nextCursor
	zerolog.Ctx(ctx).Info().
		Int("page", pager.pages).
		Int("page_messages", len(response.Messages)).
		Int("new_messages", len(messages)).
		Int("unique_messages", len(pager.seenMessages)).
		Int64("reported_total_messages", pager.totalMessages).
		Time("oldest_page_message_ts", time.UnixMicro(oldest.Timestamp)).
		Time("newest_page_message_ts", time.UnixMicro(newest)).
		Msg("Fetched forward history page")
	return messages, nil
}

func collectInitialHistory(ctx context.Context, pager *historyPager, pending *InitialHistoryPending, saveUpperTimestamp func(int64) error) ([]*gmproto.Message, error) {
	messages := make([]*gmproto.Message, 0)
	for !pager.done && len(messages) < pending.Limit {
		page, err := pager.next(ctx)
		if err != nil {
			return nil, err
		}
		if len(page) == 0 {
			break
		}
		// A page is newest first, but sorting also handles timestamp ties and page overlap.
		slices.SortStableFunc(page, func(left, right *gmproto.Message) int {
			return cmp.Compare(right.Timestamp, left.Timestamp)
		})
		if pending.UpperTimestamp == nil {
			upperTimestamp := page[0].Timestamp
			if err = saveUpperTimestamp(upperTimestamp); err != nil {
				return nil, err
			}
			pending.UpperTimestamp = &upperTimestamp
		}
		for _, message := range page {
			if message.Timestamp <= *pending.UpperTimestamp {
				messages = append(messages, message)
				if len(messages) == pending.Limit {
					break
				}
			}
		}
	}
	slices.SortStableFunc(messages, func(left, right *gmproto.Message) int {
		return cmp.Compare(left.Timestamp, right.Timestamp)
	})
	return messages, nil
}

func collectCatchupHistory(ctx context.Context, pager *historyPager, completed *HistoryPosition, limit int) ([]*gmproto.Message, error) {
	messages := make([]*gmproto.Message, 0)
	for !pager.done {
		page, err := pager.next(ctx)
		if err != nil {
			return nil, err
		}
		reachedCompleted := false
		for _, message := range page {
			if message.Timestamp < completed.Timestamp {
				reachedCompleted = true
				continue
			}
			if message.Timestamp == completed.Timestamp && slices.Contains(completed.MessageIDs, message.MessageID) {
				continue
			}
			messages = append(messages, message)
			if len(messages) > limit {
				return nil, fmt.Errorf("%w (limit %d)", ErrCatchupLimit, limit)
			}
		}
		// The original anchor may have been deleted. Crossing its timestamp is sufficient.
		// Equality is not sufficient: other IDs at the same timestamp must be collected.
		if reachedCompleted {
			break
		}
	}
	slices.SortStableFunc(messages, func(left, right *gmproto.Message) int {
		return cmp.Compare(left.Timestamp, right.Timestamp)
	})
	return messages, nil
}

func advanceHistoryPosition(previous *HistoryPosition, messages []*gmproto.Message) *HistoryPosition {
	position := &HistoryPosition{}
	if previous != nil {
		position.Timestamp = previous.Timestamp
		position.MessageIDs = slices.Clone(previous.MessageIDs)
	}
	for _, message := range messages {
		if message.Timestamp > position.Timestamp {
			position.Timestamp = message.Timestamp
			position.MessageIDs = nil
		}
		if message.Timestamp == position.Timestamp && !slices.Contains(position.MessageIDs, message.MessageID) {
			position.MessageIDs = append(position.MessageIDs, message.MessageID)
		}
	}
	return position
}
