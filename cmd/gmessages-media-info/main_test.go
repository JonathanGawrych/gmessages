// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"

	"go.mau.fi/mautrix-gmessages/pkg/libgm/gmproto"
	"google.golang.org/protobuf/proto"
)

func TestInspectSavedMediaDoesNotPrintPrivateFields(t *testing.T) {
	message := &gmproto.Message{MessageID: "private-message", ConversationID: "private-chat", Timestamp: 1700000000000000,
		MessageInfo: []*gmproto.MessageInfo{{ActionMessageID: proto.String("private-part"), Data: &gmproto.MessageInfo_MediaContent{
			MediaContent: &gmproto.MediaContent{MediaID: "private-media", MediaName: "private-name", DecryptionKey: []byte("private-key")},
		}}},
	}
	for _, input := range []proto.Message{message, &gmproto.UpdateEvents{Event: &gmproto.UpdateEvents_MessageEvent{MessageEvent: &gmproto.MessageEvent{Data: []*gmproto.Message{message}}}}} {
		raw, err := proto.Marshal(input)
		if err != nil {
			t.Fatal(err)
		}
		summary, err := inspectSavedMedia(base64.StdEncoding.EncodeToString(raw))
		if err != nil {
			t.Fatal(err)
		}
		if summary.Messages != 1 || len(summary.Attachments) != 1 || !summary.Attachments[0].FullID || !summary.Attachments[0].FullKey || summary.Attachments[0].ThumbnailID {
			t.Fatalf("incorrect presence summary: %+v", summary)
		}
		encoded, err := json.Marshal(summary)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(encoded), "private-") {
			t.Fatal("summary exposed private source data")
		}
	}
	if _, err := inspectSavedMedia("invalid!"); err == nil {
		t.Fatal("invalid raw data accepted")
	}
}
