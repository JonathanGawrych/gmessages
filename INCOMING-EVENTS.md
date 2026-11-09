# Durable incoming Google events

Long history imports run inside a serialized portal handler. Previously, the
Google long-poll reader also called portal enqueue synchronously. A full portal
channel stopped that reader, including the command reply the import was waiting
for. Increasing the channel capacity only moved the failure to a later page.

This fork commits unsolicited RPCs and all their derived event rows to the
existing bridge database before queuing their Google acknowledgement. Command
replies with a matching SessionID waiter bypass the event store. Conversation,
message and typing updates use one FIFO stream per conversation; account and
other control updates use a separate FIFO stream. One callback per stream runs
at a time, with one loaded event and a single wake notification in memory.
The existing bounded portal channel is unchanged. Conversation updates no longer
start a separate goroutine that could overtake another event in their stream.

The pinned framework normally backgrounds a portal handler after five minutes
without cancelling it. That allows later events, including another history sync,
to run concurrently with the first import. This connector sets the exported
`EventHandlingTimeoutTicks` to `math.MaxInt` once during initialization, effectively
disabling that timeout for this bridge process. It overrides
`MAUTRIX_EVENT_HANDLING_TIMEOUT_TICKS`; `bridge.async_events` must be `false`, or
startup fails. The existing warning every 30 seconds remains.

A slow or stuck handler therefore keeps its own portal blocked until it finishes
or the process stops. Other portals keep their independent workers, and command
replies continue through the reader. This preserves ordering through raw history
fetch, media conversion, Matrix sends and checkpoint completion, rather than
only preventing simultaneous fetches. No dependency fork is required.

## Receipt and failure guarantees

The ResponseID used by Google's acknowledgement request identifies a whole RPC.
Its receipt and every child row are committed in one transaction. Receipts retain
a logical-content SHA-256 digest after their children finish; their raw payload
is cleared under a parent-row lock. A duplicate RPC cannot recreate children that
already finished, even after restart. The digest excludes outer execution/timing
fields, encryption randomness and the connection-local IsOld flag. The original
IsOld value is retained. Reuse of the same ResponseID with different logical
content fails visibly without acknowledgement; the implementation does not
assume that different content is safe to discard.

An event row is removed only after its callback returns successfully. That return
normally means **portal enqueue, not Matrix commit**. A process crash after portal
enqueue can lose already acknowledged events from that in-memory portal channel,
as in the original bridge. A crash before deleting a child can replay a callback
that already enqueued an event. This is not end-to-end exactly-once delivery.
Forward-history checkpoints and pending imports retain their separate guarantees.

A failed enqueue is not acknowledged and closes the long-poll response so Google
can redeliver it. Commit followed by a crash before acknowledgement is handled by
receipt deduplication. A callback error, panic, cancelled callback or failed child
deletion leaves the row and later rows in that stream for retry. Errors are logged;
a later event or reconnect retries the stream. Other conversation streams continue.
Existing intentional ignores (such as stale typing) remain successful callbacks.
Existing delayed conversation resync/receipt work and asynchronous session updates
retain their previous completion boundary.

Disconnect cancels phone waiters first, then cancels and joins the stream workers.
Reconnect waits for those callbacks to return before starting replacement workers.
It also joins the entire old polling loop, including connection cleanup. Polling
requests and retry waits use the dispatcher's cancellation context. A reader keeps
the dispatcher captured when its goroutine was launched; a retired reader cannot
consume the replacement connection's startup count or update its receive timer.
This can delay disconnect while an existing portal enqueue is blocked. Logout
callbacks request session invalidation from a separate goroutine. Deleting a login
cascades to that login's receipts and pending events.

## Storage and rollback

The gmessages_version schema changes from v1 to v2, with compatibility set to v2.
Older bridge builds reject that schema. **An image-only rollback is not supported.**
Take a consistent backup before migration. Preserve pending events and receipts,
or drain them and make an explicit database migration/restore decision before
returning to an older build. Do not bypass the schema-version check.

Pending payloads contain decrypted protobuf message content and must be treated
as private message data. Completed receipts retain only IDs and a 32-byte digest.
Receipts grow with RPC count; pending disk usage grows while a stream is blocked.
There is no unbounded in-memory event buffer. Database write failures stop
acknowledgement and are reported, rather than dropping events.

## Tests

`go test ./...` runs synthetic dispatcher and isolated SQLite tests. For the same
store tests on PostgreSQL, provide a separate database whose name starts with
`gmessages_inbox_test`, using a keyword-format DSN:

```
GMESSAGES_TEST_POSTGRES_DSN='host=/var/run/postgresql user=postgres dbname=gmessages_inbox_test sslmode=disable' go test -run '^TestIncomingStorePostgres$' -v ./pkg/connector/gmdb
```

The PostgreSQL suite creates a randomly named schema and removes only that schema
afterward. It opens multiple connections to test concurrent completion and parent
row locking. `go test -c ./pkg/connector/gmdb` can produce a standalone test binary;
run it with the same environment variable and `-test.run`/`-test.v` flags.
