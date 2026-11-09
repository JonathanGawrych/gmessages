// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"image"
	"image/png"
	"strings"
	"testing"

	"go.mau.fi/mautrix-gmessages/pkg/libgm/gmproto"
	"google.golang.org/protobuf/proto"
)

func TestInspectSavedMediaDoesNotPrintPrivateFields(t *testing.T) {
	inline := &bytes.Buffer{}
	if err := png.Encode(inline, image.NewRGBA(image.Rect(0, 0, 2, 3))); err != nil {
		t.Fatal(err)
	}
	message := &gmproto.Message{MessageID: "private-message", ConversationID: "private-chat", Timestamp: 1700000000000000,
		MessageInfo: []*gmproto.MessageInfo{{ActionMessageID: proto.String("private-part"), Data: &gmproto.MessageInfo_MediaContent{
			MediaContent: &gmproto.MediaContent{MediaID: "private-media", MediaName: "private-name", DecryptionKey: []byte("private-key"), MediaData: inline.Bytes()},
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
		if summary.Attachments[0].InlineFormat != "png" || summary.Attachments[0].InlineWidth != 2 || summary.Attachments[0].InlineHeight != 3 {
			t.Fatalf("incorrect inline dimensions: %+v", summary.Attachments[0])
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
