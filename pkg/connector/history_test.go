// SPDX-License-Identifier: AGPL-3.0-or-later

package connector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"

	"go.mau.fi/mautrix-gmessages/pkg/libgm/gmproto"
)

func historyTestMessage(number int) *gmproto.Message {
	return &gmproto.Message{MessageID: fmt.Sprint(number), Timestamp: 1700000000000000 + int64(number)*1000}
}

func historyTestCursor(message *gmproto.Message) *gmproto.Cursor {
	return &gmproto.Cursor{LastItemID: message.MessageID, LastItemTimestamp: time.UnixMicro(message.Timestamp).UnixMilli()}
}

func historyTestMessages(newest, oldest int) []*gmproto.Message {
	messages := make([]*gmproto.Message, 0)
	for number := newest; number >= oldest; number-- {
		messages = append(messages, historyTestMessage(number))
	}
	return messages
}

type historyTestPage struct {
	cursor   *gmproto.Cursor
	response *gmproto.ListMessagesResponse
	err      error
}

func historyTestPager(t *testing.T, pages []historyTestPage) *historyPager {
	t.Helper()
	calls := 0
	t.Cleanup(func() {
		if calls != len(pages) {
			t.Errorf("got %d requests, want %d", calls, len(pages))
		}
	})
	return newHistoryPager(func(ctx context.Context, conversationID string, count int64, cursor *gmproto.Cursor) (*gmproto.ListMessagesResponse, error) {
		t.Helper()
		if count != 100 || conversationID != "test" || calls >= len(pages) {
			t.Fatalf("unexpected fetch: count=%d conversation=%q page=%d", count, conversationID, calls)
		}
		page := pages[calls]
		calls++
		if !proto.Equal(cursor, page.cursor) {
			t.Fatalf("page %d cursor = %v, want %v", calls, cursor, page.cursor)
		}
		return page.response, page.err
	}, "test")
}

func collectTestInitial(ctx context.Context, pager *historyPager, pending *InitialHistoryPending) ([]*gmproto.Message, error) {
	return collectInitialHistory(ctx, pager, pending, func(timestamp int64) error {
		pending.UpperTimestamp = &timestamp
		return nil
	})
}

func TestInitialHistoryMultiplePagesOverlapAndEmptyEnd(t *testing.T) {
	first := historyTestMessages(250, 151)
	second := historyTestMessages(151, 52)
	third := historyTestMessages(52, 1)
	firstCursor := historyTestCursor(first[len(first)-1])
	secondCursor := historyTestCursor(second[len(second)-1])
	thirdCursor := historyTestCursor(third[len(third)-1])
	pager := historyTestPager(t, []historyTestPage{
		{response: &gmproto.ListMessagesResponse{Messages: first, Cursor: firstCursor, TotalMessages: 250}},
		{cursor: firstCursor, response: &gmproto.ListMessagesResponse{Messages: second, Cursor: secondCursor}},
		{cursor: secondCursor, response: &gmproto.ListMessagesResponse{Messages: third}},
		{cursor: thirdCursor, response: &gmproto.ListMessagesResponse{}},
	})
	result, err := collectTestInitial(context.Background(), pager, &InitialHistoryPending{Limit: 1000})
	if err != nil || len(result) != 250 || !pager.done || pager.totalMessages != 250 {
		t.Fatalf("count/done/total/error = %d/%t/%d/%v", len(result), pager.done, pager.totalMessages, err)
	}
	for index, message := range result {
		if !proto.Equal(message, historyTestMessage(index+1)) {
			t.Fatalf("message %d was changed, lost or reordered: %v", index, message)
		}
	}
	if result[249] != first[0] {
		t.Fatal("raw message object was replaced")
	}
}

func TestInitialHistoryHonorsBoundAndRetainsInitialSnapshotOnRetry(t *testing.T) {
	upper := historyTestMessage(250).Timestamp
	state := ForwardHistoryMetadata{Initial: &InitialHistoryPending{Limit: 5, UpperTimestamp: &upper}}
	serialized, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	var restored ForwardHistoryMetadata
	if err = json.Unmarshal(serialized, &restored); err != nil {
		t.Fatal(err)
	}
	pager := historyTestPager(t, []historyTestPage{{response: &gmproto.ListMessagesResponse{Messages: historyTestMessages(260, 230)}}})
	result, err := collectTestInitial(context.Background(), pager, restored.Initial)
	if err != nil || len(result) != 5 || pager.done {
		t.Fatalf("count/done/error = %d/%t/%v", len(result), pager.done, err)
	}
	if result[0].MessageID != "246" || result[4].MessageID != "250" {
		t.Fatalf("retry used newer live messages instead of initial upper timestamp: %v", result)
	}
	if restored.Completed != nil || restored.CatchupPending || restored.Initial.Limit != 5 {
		t.Fatalf("initial state changed to catch-up: %+v", restored)
	}
}

func TestInitialHistoryEmpty(t *testing.T) {
	pager := historyTestPager(t, []historyTestPage{{response: &gmproto.ListMessagesResponse{}}})
	result, err := collectTestInitial(context.Background(), pager, &InitialHistoryPending{Limit: 10})
	if err != nil || len(result) != 0 || !pager.done {
		t.Fatalf("result/done/error = %v/%t/%v", result, pager.done, err)
	}
}

func TestCatchupHistoryPagesUntilCompletedTimestamp(t *testing.T) {
	for _, deletedAnchor := range []bool{false, true} {
		t.Run(fmt.Sprintf("deleted_anchor=%t", deletedAnchor), func(t *testing.T) {
			first := historyTestMessages(250, 151)
			second := historyTestMessages(151, 52)
			if deletedAnchor {
				second = slices.DeleteFunc(second, func(message *gmproto.Message) bool { return message.MessageID == "100" })
			}
			cursor := historyTestCursor(first[len(first)-1])
			pager := historyTestPager(t, []historyTestPage{
				{response: &gmproto.ListMessagesResponse{Messages: first, Cursor: cursor}},
				{cursor: cursor, response: &gmproto.ListMessagesResponse{Messages: second}},
			})
			completed := &HistoryPosition{Timestamp: historyTestMessage(100).Timestamp, MessageIDs: []string{"100"}}
			result, err := collectCatchupHistory(context.Background(), pager, completed, 150)
			if err != nil || len(result) != 150 || result[0].MessageID != "101" || result[149].MessageID != "250" {
				t.Fatalf("result/error = %v/%v", result, err)
			}
			if completed.MessageIDs[0] != "100" {
				t.Fatal("collection advanced completed position before sends")
			}
		})
	}
}

func TestCatchupLimitFailsWithoutPartialResult(t *testing.T) {
	completed := &HistoryPosition{Timestamp: historyTestMessage(1).Timestamp, MessageIDs: []string{"1"}}
	pager := historyTestPager(t, []historyTestPage{{response: &gmproto.ListMessagesResponse{Messages: historyTestMessages(20, 1)}}})
	result, err := collectCatchupHistory(context.Background(), pager, completed, 10)
	if result != nil || !errors.Is(err, ErrCatchupLimit) || completed.MessageIDs[0] != "1" {
		t.Fatalf("result/error/position = %v/%v/%v", result, err, completed)
	}
}

func TestBootstrapExistingRoomDoesNotUseInitialWindow(t *testing.T) {
	baseline := &HistoryPosition{Timestamp: historyTestMessage(100).Timestamp, MessageIDs: []string{"100"}}
	state := &ForwardHistoryMetadata{Completed: baseline, BootstrapAnchor: baseline, CatchupPending: true}
	firstCursor := historyTestCursor(historyTestMessage(251))
	secondCursor := historyTestCursor(historyTestMessage(152))
	pager := historyTestPager(t, []historyTestPage{
		{response: &gmproto.ListMessagesResponse{Messages: historyTestMessages(350, 251)}},
		{cursor: firstCursor, response: &gmproto.ListMessagesResponse{Messages: historyTestMessages(251, 152)}},
		{cursor: secondCursor, response: &gmproto.ListMessagesResponse{Messages: historyTestMessages(152, 53)}},
	})
	result, err := collectCatchupHistory(context.Background(), pager, state.Completed, 500)
	if err != nil || len(result) != 250 || result[0].MessageID != "101" || result[249].MessageID != "350" {
		t.Fatalf("bootstrap failed to recover outage gap: %v/%v", result, err)
	}
	if state.Initial != nil || state.BootstrapAnchor != baseline || state.Completed != baseline {
		t.Fatal("bootstrap was replaced by an initial window or advanced before verification")
	}
}

func TestEqualTimestampsWithDistinctIDsAcrossPages(t *testing.T) {
	first := historyTestMessage(3)
	second := historyTestMessage(2)
	third := historyTestMessage(1)
	second.Timestamp = first.Timestamp
	third.Timestamp = first.Timestamp
	firstCursor := historyTestCursor(second)
	secondCursor := historyTestCursor(third)
	for _, useFallback := range []bool{false, true} {
		t.Run(fmt.Sprintf("fallback=%t", useFallback), func(t *testing.T) {
			firstResponse := &gmproto.ListMessagesResponse{Messages: []*gmproto.Message{first, second}}
			secondResponse := &gmproto.ListMessagesResponse{Messages: []*gmproto.Message{second, third}}
			if !useFallback {
				firstResponse.Cursor = firstCursor
				secondResponse.Cursor = secondCursor
			}
			pager := historyTestPager(t, []historyTestPage{
				{response: firstResponse},
				{cursor: firstCursor, response: secondResponse},
				{cursor: secondCursor, response: &gmproto.ListMessagesResponse{}},
			})
			completed := &HistoryPosition{Timestamp: first.Timestamp, MessageIDs: []string{"2"}}
			result, err := collectCatchupHistory(context.Background(), pager, completed, 2)
			if err != nil || len(result) != 2 {
				t.Fatalf("result/error = %v/%v", result, err)
			}
			position := advanceHistoryPosition(completed, result)
			for _, id := range []string{"1", "2", "3"} {
				if !slices.Contains(position.MessageIDs, id) {
					t.Fatalf("equal timestamp lost ID %s", id)
				}
			}
			if len(completed.MessageIDs) != 1 {
				t.Fatal("position advancement mutated previous completed state")
			}
		})
	}
}

func TestHistoryRejectsNonAdvancingPagination(t *testing.T) {
	first := historyTestMessage(3)
	second := historyTestMessage(2)
	third := historyTestMessage(1)
	firstCursor := historyTestCursor(first)
	secondCursor := historyTestCursor(second)
	firstPage := historyTestPage{response: &gmproto.ListMessagesResponse{Messages: []*gmproto.Message{first}, Cursor: firstCursor}}
	for _, testCase := range []struct {
		name  string
		pages []historyTestPage
		want  string
	}{
		{"unchanged", []historyTestPage{firstPage, {cursor: firstCursor, response: &gmproto.ListMessagesResponse{Messages: []*gmproto.Message{second}, Cursor: firstCursor}}}, "repeated a cursor"},
		{"loop", []historyTestPage{firstPage, {cursor: firstCursor, response: &gmproto.ListMessagesResponse{Messages: []*gmproto.Message{second}, Cursor: secondCursor}}, {cursor: secondCursor, response: &gmproto.ListMessagesResponse{Messages: []*gmproto.Message{third}, Cursor: firstCursor}}}, "repeated a cursor"},
		{"newer", []historyTestPage{firstPage, {cursor: firstCursor, response: &gmproto.ListMessagesResponse{Messages: []*gmproto.Message{second}, Cursor: historyTestCursor(historyTestMessage(4))}}}, "newer timestamp"},
		{"duplicates", []historyTestPage{firstPage, {cursor: firstCursor, response: &gmproto.ListMessagesResponse{Messages: []*gmproto.Message{first}, Cursor: secondCursor}}}, "no message ID progress"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			result, err := collectTestInitial(context.Background(), historyTestPager(t, testCase.pages), &InitialHistoryPending{Limit: 100})
			if result != nil || err == nil || !strings.Contains(err.Error(), testCase.want) {
				t.Fatalf("result/error = %v/%v, want nil and %q", result, err, testCase.want)
			}
		})
	}
}

func TestHistoryFetchAndCheckpointErrors(t *testing.T) {
	wantError := errors.New("synthetic error")
	cursor := historyTestCursor(historyTestMessage(1))
	pager := historyTestPager(t, []historyTestPage{
		{response: &gmproto.ListMessagesResponse{Messages: historyTestMessages(2, 1)}},
		{cursor: cursor, err: wantError},
	})
	result, err := collectTestInitial(context.Background(), pager, &InitialHistoryPending{Limit: 100})
	if result != nil || !errors.Is(err, wantError) {
		t.Fatalf("result/error = %v/%v", result, err)
	}
	pager = historyTestPager(t, []historyTestPage{{response: &gmproto.ListMessagesResponse{Messages: historyTestMessages(2, 1)}}})
	result, err = collectInitialHistory(context.Background(), pager, &InitialHistoryPending{Limit: 100}, func(int64) error { return wantError })
	if result != nil || !errors.Is(err, wantError) {
		t.Fatalf("failed initial boundary save returned partial history: %v/%v", result, err)
	}
}

func TestHistoryCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	result, err := collectTestInitial(ctx, historyTestPager(t, nil), &InitialHistoryPending{Limit: 100})
	if result != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("result/error = %v/%v", result, err)
	}
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	pager := newHistoryPager(func(fetchContext context.Context, _ string, _ int64, _ *gmproto.Cursor) (*gmproto.ListMessagesResponse, error) {
		if fetchContext != ctx {
			t.Fatal("request did not receive caller context")
		}
		cancel()
		return &gmproto.ListMessagesResponse{}, nil
	}, "test")
	result, err = collectTestInitial(ctx, pager, &InitialHistoryPending{Limit: 100})
	if result != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("result/error = %v/%v", result, err)
	}
}

func TestPendingHistoryDoesNotUseLatestLiveMessageAsCompleted(t *testing.T) {
	conversation := &gmproto.Conversation{LatestMessageID: "200", LastMessageTimestamp: historyTestMessage(200).Timestamp}
	completed := &HistoryPosition{Timestamp: conversation.LastMessageTimestamp, MessageIDs: []string{"200"}}
	for _, state := range []*ForwardHistoryMetadata{
		nil,
		{Initial: &InitialHistoryPending{Limit: 1000}},
		{Completed: completed, CatchupPending: true},
		{Completed: &HistoryPosition{Timestamp: historyTestMessage(100).Timestamp, MessageIDs: []string{"100"}}},
		{Completed: &HistoryPosition{Timestamp: conversation.LastMessageTimestamp, MessageIDs: []string{"other-id"}}},
	} {
		if !needsForwardHistory(state, conversation) {
			t.Fatalf("pending or incomplete history was hidden by live message: %+v", state)
		}
	}
	if needsForwardHistory(&ForwardHistoryMetadata{Completed: completed}, conversation) {
		t.Fatal("completed history unnecessarily retriggered")
	}
}

func TestHistoryPartialMessageRecoveryAndVerification(t *testing.T) {
	message := &bridgev2.BackfillMessage{
		ID:               "test-message",
		ConvertedMessage: &bridgev2.ConvertedMessage{Parts: []*bridgev2.ConvertedMessagePart{{ID: "text"}, {ID: "media"}, {ID: "ignored", DontBridge: true}}},
	}
	stored := []*database.Message{{PartID: "text", MXID: "$text"}}
	lookup := func(networkid.MessageID) ([]*database.Message, error) { return stored, nil }
	filtered, expected, err := filterStoredHistoryParts([]*bridgev2.BackfillMessage{message}, lookup)
	if err != nil || len(filtered) != 1 || len(filtered[0].Parts) != 2 || filtered[0].Parts[0].ID != "media" || len(message.Parts) != 3 {
		t.Fatalf("partial message recovery failed: %v/%v", filtered, err)
	}
	if err = verifyStoredHistoryParts(expected, lookup); err == nil {
		t.Fatal("checkpoint accepted a partially stored message")
	}
	stored = append(stored, &database.Message{PartID: "media", MXID: "$media"})
	if err = verifyStoredHistoryParts(expected, lookup); err == nil {
		t.Fatal("checkpoint ignored the missing DontBridge database row")
	}
	stored = append(stored, &database.Message{PartID: "ignored", MXID: "$fake"})
	if err = verifyStoredHistoryParts(expected, lookup); err != nil {
		t.Fatal(err)
	}
	filtered, _, err = filterStoredHistoryParts([]*bridgev2.BackfillMessage{message}, lookup)
	if err != nil || len(filtered) != 0 {
		t.Fatalf("already stored message would be resent: %v/%v", filtered, err)
	}
	wantError := errors.New("synthetic database error")
	failingLookup := func(networkid.MessageID) ([]*database.Message, error) { return nil, wantError }
	if err = verifyStoredHistoryParts(expected, failingLookup); !errors.Is(err, wantError) {
		t.Fatalf("database verification failure was hidden: %v", err)
	}
	if _, _, err = filterStoredHistoryParts([]*bridgev2.BackfillMessage{message}, failingLookup); !errors.Is(err, wantError) {
		t.Fatalf("database lookup failure was hidden: %v", err)
	}
}

func TestCompletedPositionWaitsForVerifiedStorageAndDurableSave(t *testing.T) {
	for _, initial := range []bool{false, true} {
		t.Run(fmt.Sprintf("initial=%t", initial), func(t *testing.T) {
			state := &ForwardHistoryMetadata{
				Completed:      &HistoryPosition{Timestamp: 10, MessageIDs: []string{"old"}},
				CatchupPending: true,
			}
			if initial {
				state = &ForwardHistoryMetadata{Initial: &InitialHistoryPending{Limit: 1000}}
			}
			original := state
			position := &HistoryPosition{Timestamp: 20, MessageIDs: []string{"new"}}
			next := &ForwardHistoryMetadata{Completed: position}
			expected := expectedHistoryParts{"new": {""}}
			stored := []*database.Message{}
			lookup := func(networkid.MessageID) ([]*database.Message, error) { return stored, nil }
			saves := 0
			save := func(next *ForwardHistoryMetadata) error {
				saves++
				state = next
				return nil
			}
			if err := completeForwardHistory(context.Background(), expected, lookup, save, next); err == nil || state != original || saves != 0 {
				t.Fatalf("unsaved message advanced checkpoint: %v", err)
			}
			serialized, err := json.Marshal(state)
			if err != nil {
				t.Fatal(err)
			}
			var restarted ForwardHistoryMetadata
			if err = json.Unmarshal(serialized, &restarted); err != nil {
				t.Fatal(err)
			}
			if !needsForwardHistory(&restarted, &gmproto.Conversation{LatestMessageID: "new", LastMessageTimestamp: 20}) {
				t.Fatal("restart lost unfinished history")
			}
			stored = []*database.Message{{PartID: "", MXID: "$new"}}
			wantError := errors.New("synthetic checkpoint save error")
			if err = completeForwardHistory(context.Background(), expected, lookup, func(*ForwardHistoryMetadata) error { return wantError }, next); !errors.Is(err, wantError) || state != original {
				t.Fatalf("failed checkpoint save changed state: %v", err)
			}
			if err = completeForwardHistory(context.Background(), expected, lookup, save, next); err != nil || saves != 1 || state.Initial != nil || state.CatchupPending || state.Completed != position {
				t.Fatalf("successful completion not saved: %v/%+v", err, state)
			}
		})
	}
}
