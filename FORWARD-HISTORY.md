# Forward history pagination

Long imports also require the [durable incoming-event dispatcher](INCOMING-EVENTS.md).
Its schema migration and rollback restriction are separate from history metadata.

This fork keeps the v0.2608.0 bridge and mautrix-go v0.30.0 dependencies. Forward
history requests use 100-message Google pages, follow response cursors, and use
the oldest message as a cursor when Google returns `null`. Short pages are not
treated as the end of history. Message IDs are deduplicated across pages, and
equal timestamps with different IDs remain eligible.

Initial import and catch-up use separate limits:

- `backfill.max_initial_messages` is the maximum number of recent messages in a
  new room's initial window. A failed initial import retains this limit and its
  upper timestamp across restart, even if newer live messages arrive.
- `backfill.max_catchup_messages` is a hard limit on an unfinished catch-up gap.
  If the gap exceeds it, the bridge reports an error and keeps the old completed
  position. Raising the limit permits a later resync to retry the same gap.
  Reaching the limit does not silently declare newer history complete.
- A value of zero continues to disable the corresponding initial or catch-up
  operation. Global `backfill.enabled` still applies.

All requested raw pages must succeed before conversion or Matrix message sends
start. Messages are converted and sent oldest first. Repeated cursors, a cursor
moving to a newer timestamp, and a nonempty page with no new message IDs are
errors. Cancellation and fetch errors return no partial collection.

## Completed history and recovery

Portal JSON metadata contains `forward_history`. `completed` stores a timestamp
in microseconds and the processed message IDs at that timestamp. Live delivery
does not update it. `initial` retains an unfinished initial import's `limit` and
`upper_timestamp_us`; `catchup_pending` records unfinished catch-up work.

Before connecting a login to Google, existing rooms without this metadata get a
baseline from their latest persisted message. `bootstrap_anchor` records that
baseline permanently. History before it is unverified: installing this fork
cannot establish whether the older bridge previously skipped messages. An
existing database ending at message 100 with Google now at message 350 catches
up from 100 using the catch-up limit, regardless of a smaller initial limit.
Empty rooms retain the normal initial-window behavior.

Catch-up runs inside the portal's existing serialized chat-resync handling.
It uses the stored completed position instead of mautrix-go's latest-live-message
cutoff. The pinned `Portal.Internal().SendBackfill` method preserves the existing
Matrix sending behavior. This dependency must be reviewed when upgrading
mautrix-go.

A successful send callback alone is insufficient. The bridge checks every
converted message part, including intentionally unbridged parts, against the
message database before saving a new completed position. A missing part or
failed database read/save leaves the operation pending. Retries skip already
stored parts and send the remaining parts of partially stored messages.
Conversion occurs before stored-part filtering, so retries can download and
upload already stored attachments again. A message-ID-only shortcut would lose
unstored parts of partially stored messages. Progress logs mark raw-fetch
completion, then report conversion progress every 100 messages and at the end.

Deleting the original anchor message does not prevent catch-up: paging continues
until it crosses the completed timestamp or reaches the end. Different IDs at
the completed timestamp are still imported.

## Limits of the guarantee

Each successful attempt sends its messages chronologically. After a failed
Matrix send, live messages can be delivered before the unfinished history is
retried. Consumers must use original message timestamps and must not assume the
room's insertion order is chronological across failures.

The original media converter is unchanged. Unavailable attachments can become
`Waiting for attachment ...` or `Failed to transfer attachment ...` notice
messages with `MediaPending=true`. Such a stored notice counts as the original
converter's output, not proof that full-resolution media transferred. Search
for `Failed to copy attachment` when auditing media. Thumbnail-only results may
request full-size media asynchronously.

If Matrix accepted an event but its database insert failed, a retry can duplicate
that event because its Matrix event ID was not saved. The completed position
still does not advance. Older builds do not understand this metadata and may
discard it when saving portals; preserve it before downgrading if recovery will
continue later.

## Tests

`go test ./pkg/connector` covers multi-page imports, boundary overlap, short pages,
cursor fallback, timestamp ties, deleted anchors, explicit limits, initial
snapshot restoration, cancellation, paging errors, pending-state checks, partial
message recovery, and completed-position verification. The tests use synthetic
messages and do not connect to Google or Matrix.
