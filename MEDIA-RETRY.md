# Targeted pending attachment retry

After correcting a Matrix upload limit or a temporary transfer failure, a bridge
administrator can run `!gm retry-media <original Matrix event ID>` in that event's
portal room. Alternatively, reply to the original message with `!gm retry-media`.
Use the configured command prefix if it is not `!gm`. Commands are intercepted by
the bridge and are not sent to Google Messages. Replies are bot notices.

The command uses the administrator's existing login for that portal and the
original Matrix sender's native bridge intent. It queues an edit behind earlier
portal events. The framework applies its usual ghost/double-puppet echo filtering.
No message is sent, edited, deleted or marked read on the phone.

Only stored parts with `media_pending=true` are selected. Successful parts are
not downloaded, uploaded or edited. The first stored part supplies the saved
`fi.mau.gmessages.raw_debug_data` protobuf, even if the specified event is another
attachment. Account, conversation, message, timestamp, part IDs and known media
IDs must match the stored records. Available text hashes and part counts are also
checked for stale data. Invalid or mismatched saved raw data is an explicit error.
When the field is absent or an empty string, a bounded read-only lookup can supply
the source message, as described below. Download keys are never guessed.

The command preserves the original message/part IDs, sender, timestamp, merged
caption and unrelated message metadata. Each successful transfer becomes a native
Matrix replacement of that part. A transfer failure leaves the part pending;
Matrix send or database save failures also leave its stored pending flag intact.
The completion notice reads the database again, reporting partial success rather
than assuming that a handler return means every edit was saved. Retrying selects
only the parts still pending. As with other bridge edits, an accepted Matrix edit
whose database save failed can be sent again on retry.

This command retries saved media IDs, including a saved thumbnail ID if that is
all the original data contains. The completion notice counts thumbnail-only
replacements explicitly. It does not request full-size media from the phone.
Parts with no saved media ID need a separate recovery path and remain pending.
Saved raw data contains private message data and media download keys; do not paste
it into logs, issues or public reports.

The pinned Matrix connector caches the homeserver upload limit at startup. Raise
the homeserver limit first, then restart the bridge after any active import has
finished before invoking the retry. A command queued in memory is not preserved
through a restart; its stored pending flags remain available for another command.
No database migration or dependency change is required.

## Request one attachment without a media ID

An administrator can run `!gm request-media <original attachment event ID>` in
that attachment's portal, or reply to that attachment notice with `!gm request-media`.
It selects exactly one stored pending attachment, not every part of the message.
Successful attachments, text parts and parts with saved media IDs are rejected.
The first stored part's raw protobuf must match the account, conversation,
message, timestamp and attachment part ID. An absent or empty raw field uses the
same bounded lookup; invalid or stale saved data remains an explicit error.

The command enters the same serialized portal queue and calls the existing paired
client's `GetFullSizeImage(messageID, actionMessageID)` once. It does not send text,
read receipts or typing actions, re-pair a client, download/upload media, edit
Matrix messages, or change stored pending flags. The protobuf response is empty:
a successful return is only acknowledgment, not proof that a file is available.
Actual recovery requires a later unsolicited Google message update, the ordinary
native Matrix edit, and a successfully saved `media_pending=false` record. The
native update path can process other parts of that same message; this command does
not change that existing behavior. Testing a message containing one attachment
keeps the initial experiment limited to one file.

Messages with no media IDs can still contain inline image bytes. The normal
converter's outer no-ID branch currently leaves those parts pending without
decoding those bytes. This command requests full-size media and does not convert
inline previews or claim that the phone will still have the full-size file.
Request failure leaves pending state unchanged. An administrator may explicitly
repeat the command later; there is no automatic request loop or persisted command.

## One-page lookup when saved raw data was omitted

The normal converter omits `raw_debug_data` when its base64 representation would
contain 8192 or more characters. If the original first Matrix event has no such
field, or its value is an empty string, either command reads at most one page of
100 messages using the already-paired client's read-only `FetchMessages` call.
The original Matrix event's room, event ID and sender are still checked first.
A present non-string, malformed, oversized or mismatched raw value prevents this
lookup and remains an error.

The cursor uses the nearest actual stored non-fake message in the same portal
whose timestamp is in a strictly later millisecond. If there is no such message,
the command reads only the latest page. It never invents a cursor or follows the
returned cursor. A deleted target, a large gap between stored messages, or many
messages with equal timestamps can put the target outside that page. In that
case the command reports an explicit failure without scanning further history.
Exactly one returned message must match the stored account, conversation,
message ID and microsecond timestamp. Part identity, saved media IDs, available
text hashes and part counts retain their validation. Only that message enters
the selected attachment command; other page messages are not converted or sent.
Fetched source data stays in memory and is not copied into a Matrix event or log.

`request-media` still requests only one no-ID attachment. If fetched data already
has a media ID while the stored pending part has none, it reports that mismatch
and makes no full-size request, transfer, edit or metadata change. Reconciling that
state is not part of this command. `retry-media` still requires the fetched media
ID to match the stored pending media ID. Neither command silently adopts new IDs.

## Read-only saved-data inspection

`go run ./cmd/gmessages-media-info` reads one saved base64 value from standard
input. It prints only the protobuf container type, message count and per-attachment
presence of IDs/keys, plus inline data length and recognized PNG/JPEG/GIF dimensions.
It never prints the IDs, keys,
filenames or message text and makes no network or database calls. Pass the private
value through standard input, not as a command-line argument or an exported log.
