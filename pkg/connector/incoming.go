// SPDX-License-Identifier: AGPL-3.0-or-later

package connector

import (
	"context"
	"fmt"

	"maunium.net/go/mautrix/bridgev2"

	"go.mau.fi/mautrix-gmessages/pkg/connector/gmdb"
	"go.mau.fi/mautrix-gmessages/pkg/libgm"
)

func (gc *GMClient) startIncomingRPCs(ctx context.Context) error {
	gc.incomingLock.Lock()
	defer gc.incomingLock.Unlock()
	if gc.incoming != nil {
		return nil
	}
	client := gc.Client
	dispatcher, err := libgm.NewIncomingRPCDispatcher(gc.Main.br.BackgroundCtx, &gmdb.IncomingRPCStore{
		DB: gc.Main.DB, BridgeID: gc.Main.br.ID, LoginID: gc.UserLogin.ID,
	}, func(payload []byte, index int) error {
		return client.DispatchQueuedRPC(payload, index, gc.handleGMEventWithError)
	}, func(err error) {
		gc.UserLogin.Log.Error().Err(err).Msg("Incoming RPC stream stopped; pending events retained for retry")
	})
	if err != nil {
		return err
	}
	gc.incoming = dispatcher
	client.SetIncomingRPCDispatcher(dispatcher)
	gc.UserLogin.Log.Info().Msg("Durable incoming RPC dispatcher started")
	return nil
}

func (gc *GMClient) stopIncomingRPCs() {
	gc.incomingLock.Lock()
	defer gc.incomingLock.Unlock()
	if gc.incoming != nil {
		gc.UserLogin.Log.Info().Msg("Waiting for incoming RPC callbacks to stop")
		gc.incoming.Stop()
		gc.incoming = nil
	}
}

func (gc *GMClient) queueIncomingEvent(event bridgev2.RemoteEvent) error {
	result := gc.Main.br.QueueRemoteEvent(gc.UserLogin, event)
	if !result.Success {
		return fmt.Errorf("portal rejected incoming event: %v", result.Error)
	}
	return nil
}
