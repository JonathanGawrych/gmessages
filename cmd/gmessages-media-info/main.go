// SPDX-License-Identifier: AGPL-3.0-or-later

// gmessages-media-info reads one saved raw_debug_data base64 value from stdin.
// It prints media-field presence only and makes no network or database calls.
package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"image"
	_ "image/gif"
	_ "image/jpeg"
	_ "image/png"
	"io"
	"os"
	"strings"

	"google.golang.org/protobuf/proto"

	"go.mau.fi/mautrix-gmessages/pkg/libgm/gmproto"
)

type mediaPresence struct {
	PartID       bool   `json:"has_part_id"`
	FullID       bool   `json:"has_full_media_id"`
	ThumbnailID  bool   `json:"has_thumbnail_media_id"`
	FullKey      bool   `json:"has_full_media_key"`
	ThumbnailKey bool   `json:"has_thumbnail_media_key"`
	InlineBytes  int    `json:"inline_bytes"`
	InlineFormat string `json:"inline_format,omitempty"`
	InlineWidth  int    `json:"inline_width,omitempty"`
	InlineHeight int    `json:"inline_height,omitempty"`
}

type mediaSummary struct {
	RawType     string          `json:"raw_type"`
	Messages    int             `json:"messages"`
	Attachments []mediaPresence `json:"attachments"`
}

func inspectSavedMedia(encoded string) (*mediaSummary, error) {
	encoded = strings.TrimSpace(encoded)
	if encoded == "" || len(encoded) >= 8192 {
		return nil, errors.New("expected one saved base64 value shorter than 8192 characters")
	}
	raw, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, errors.New("invalid base64")
	}
	var messages []*gmproto.Message
	summary := &mediaSummary{RawType: "message", Attachments: []mediaPresence{}}
	message := &gmproto.Message{}
	if proto.Unmarshal(raw, message) == nil && message.MessageID != "" && message.ConversationID != "" && message.Timestamp > 0 {
		messages = []*gmproto.Message{message}
	} else {
		updates := &gmproto.UpdateEvents{}
		if proto.Unmarshal(raw, updates) != nil || len(updates.GetMessageEvent().GetData()) == 0 {
			return nil, errors.New("saved data is not a Message or message UpdateEvents envelope")
		}
		messages = updates.GetMessageEvent().GetData()
		summary.RawType = "update_events"
	}
	summary.Messages = len(messages)
	for _, message := range messages {
		for _, part := range message.MessageInfo {
			if media := part.GetMediaContent(); media != nil {
				presence := mediaPresence{
					PartID: part.GetActionMessageID() != "", FullID: media.MediaID != "", ThumbnailID: media.ThumbnailMediaID != "",
					FullKey: len(media.DecryptionKey) > 0, ThumbnailKey: len(media.ThumbnailDecryptionKey) > 0,
					InlineBytes: len(media.MediaData),
				}
				if dimensions, format, err := image.DecodeConfig(bytes.NewReader(media.MediaData)); err == nil {
					presence.InlineFormat = format
					presence.InlineWidth = dimensions.Width
					presence.InlineHeight = dimensions.Height
				}
				summary.Attachments = append(summary.Attachments, presence)
			}
		}
	}
	return summary, nil
}

func main() {
	encoded, err := io.ReadAll(io.LimitReader(os.Stdin, 8193))
	if err != nil {
		fmt.Fprintln(os.Stderr, "failed to read saved data from stdin")
		os.Exit(1)
	}
	summary, err := inspectSavedMedia(string(encoded))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err = json.NewEncoder(os.Stdout).Encode(summary); err != nil {
		fmt.Fprintln(os.Stderr, "failed to write media summary")
		os.Exit(1)
	}
}
