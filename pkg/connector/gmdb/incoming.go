// SPDX-License-Identifier: AGPL-3.0-or-later

package gmdb

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"

	"maunium.net/go/mautrix/bridgev2/networkid"

	"go.mau.fi/mautrix-gmessages/pkg/libgm"
)

type IncomingRPCStore struct {
	DB       *GMDB
	BridgeID networkid.BridgeID
	LoginID  networkid.UserLoginID
	lock     sync.Mutex
}

var _ libgm.IncomingRPCStore = (*IncomingRPCStore)(nil)

func (store *IncomingRPCStore) Enqueue(ctx context.Context, queued *libgm.QueuedRPC) error {
	// Serialize receipt creation for a login, including reconnecting readers.
	store.lock.Lock()
	defer store.lock.Unlock()
	return store.DB.DoTxn(ctx, nil, func(ctx context.Context) error {
		var digest []byte
		err := store.DB.QueryRow(ctx, `SELECT digest FROM gmessages_incoming_rpc WHERE bridge_id=$1 AND login_id=$2 AND response_id=$3`, store.BridgeID, store.LoginID, queued.ResponseID).Scan(&digest)
		if err == nil {
			if !bytes.Equal(digest, queued.Digest[:]) {
				return errors.New("incoming RPC response ID was reused with different logical content")
			}
			return nil
		} else if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		var payload []byte
		if len(queued.Streams) > 0 {
			payload = queued.Payload
		}
		_, err = store.DB.Exec(ctx, `INSERT INTO gmessages_incoming_rpc (bridge_id, login_id, response_id, digest, payload) VALUES ($1, $2, $3, $4, $5)`, store.BridgeID, store.LoginID, queued.ResponseID, queued.Digest[:], payload)
		if err != nil {
			return err
		}
		for index, stream := range queued.Streams {
			_, err = store.DB.Exec(ctx, `INSERT INTO gmessages_incoming_event (bridge_id, login_id, response_id, stream, part_index) VALUES ($1, $2, $3, $4, $5)`, store.BridgeID, store.LoginID, queued.ResponseID, stream, index)
			if err != nil {
				return err
			}
		}
		return nil
	})
}

func (store *IncomingRPCStore) Streams(ctx context.Context) ([]string, error) {
	rows, err := store.DB.Query(ctx, `SELECT DISTINCT stream FROM gmessages_incoming_event WHERE bridge_id=$1 AND login_id=$2`, store.BridgeID, store.LoginID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var streams []string
	for rows.Next() {
		var stream string
		if err = rows.Scan(&stream); err != nil {
			return nil, err
		}
		streams = append(streams, stream)
	}
	return streams, rows.Err()
}

func (store *IncomingRPCStore) Next(ctx context.Context, stream string) (*libgm.PendingRPC, error) {
	pending := &libgm.PendingRPC{}
	err := store.DB.QueryRow(ctx, `SELECT event.sequence, event.response_id, event.part_index, rpc.payload
		FROM gmessages_incoming_event event JOIN gmessages_incoming_rpc rpc
		ON rpc.bridge_id=event.bridge_id AND rpc.login_id=event.login_id AND rpc.response_id=event.response_id
		WHERE event.bridge_id=$1 AND event.login_id=$2 AND event.stream=$3 ORDER BY event.sequence LIMIT 1`, store.BridgeID, store.LoginID, stream).
		Scan(&pending.Sequence, &pending.ResponseID, &pending.Index, &pending.Payload)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return pending, err
}

func (store *IncomingRPCStore) Complete(ctx context.Context, pending *libgm.PendingRPC) error {
	return store.DB.DoTxn(ctx, nil, func(ctx context.Context) error {
		// Lock the receipt before deleting a child. Without this, concurrent last
		// children could both see the other child and leave the payload uncleared.
		_, err := store.DB.Exec(ctx, `UPDATE gmessages_incoming_rpc SET digest=digest WHERE bridge_id=$1 AND login_id=$2 AND response_id=$3`, store.BridgeID, store.LoginID, pending.ResponseID)
		if err != nil {
			return err
		}
		result, err := store.DB.Exec(ctx, `DELETE FROM gmessages_incoming_event WHERE bridge_id=$1 AND login_id=$2 AND sequence=$3`, store.BridgeID, store.LoginID, pending.Sequence)
		if err != nil {
			return err
		}
		deleted, err := result.RowsAffected()
		if err != nil {
			return err
		} else if deleted != 1 {
			return fmt.Errorf("queued RPC child deletion affected %d rows", deleted)
		}
		_, err = store.DB.Exec(ctx, `UPDATE gmessages_incoming_rpc SET payload=NULL WHERE bridge_id=$1 AND login_id=$2 AND response_id=$3
			AND NOT EXISTS (SELECT 1 FROM gmessages_incoming_event WHERE bridge_id=$1 AND login_id=$2 AND response_id=$3)`, store.BridgeID, store.LoginID, pending.ResponseID)
		return err
	})
}
