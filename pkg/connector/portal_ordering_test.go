// SPDX-License-Identifier: AGPL-3.0-or-later

package connector

import (
	"context"
	"database/sql"
	"math"
	"slices"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/rs/zerolog"
	"go.mau.fi/util/dbutil"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/bridgeconfig"
	"maunium.net/go/mautrix/bridgev2/commands"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/id"
)

type orderingTestMatrix struct{ bridgev2.MatrixConnector }

func (*orderingTestMatrix) Init(*bridgev2.Bridge)         {}
func (*orderingTestMatrix) BotIntent() bridgev2.MatrixAPI { return nil }

type orderingTestNetwork struct {
	bridgev2.NetworkConnector
	connector GMConnector
}

func (network *orderingTestNetwork) Init(bridge *bridgev2.Bridge) { network.connector.Init(bridge) }
func (network *orderingTestNetwork) GetDBMetaTypes() database.MetaTypes {
	return network.connector.GetDBMetaTypes()
}
func (*orderingTestNetwork) GetCapabilities() *bridgev2.NetworkGeneralCapabilities {
	return &bridgev2.NetworkGeneralCapabilities{}
}
func (*orderingTestNetwork) LoadUserLogin(_ context.Context, login *bridgev2.UserLogin) error {
	login.Client = &orderingTestClient{}
	return nil
}

type orderingTestClient struct{ bridgev2.NetworkAPI }

type orderingTestEvent struct {
	key    networkid.PortalKey
	handle func()
}

func (event orderingTestEvent) GetType() bridgev2.RemoteEventType                 { return bridgev2.RemoteEventUnknown }
func (event orderingTestEvent) GetPortalKey() networkid.PortalKey                 { return event.key }
func (event orderingTestEvent) GetSender() bridgev2.EventSender                   { return bridgev2.EventSender{} }
func (event orderingTestEvent) AddLogContext(log zerolog.Context) zerolog.Context { return log }
func (event orderingTestEvent) PreHandle(context.Context, *bridgev2.Portal)       { event.handle() }

func TestLongPortalHandlerRemainsSerialized(t *testing.T) {
	previous := bridgev2.EventHandlingTimeoutTicks
	t.Cleanup(func() { bridgev2.EventHandlingTimeoutTicks = previous })
	for _, name := range []string{"framework_default", "connector_policy"} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				raw, err := sql.Open("sqlite3", ":memory:")
				if err != nil {
					t.Fatal(err)
				}
				defer raw.Close()
				raw.SetMaxOpenConns(1)
				wrapped, err := dbutil.NewWithDB(raw, "sqlite3")
				if err != nil {
					t.Fatal(err)
				}
				network := &orderingTestNetwork{}
				// Initialization runs after framework environment configuration and
				// before loading portals, so it must replace a configured timeout.
				bridgev2.EventHandlingTimeoutTicks = 1
				bridge := bridgev2.NewBridge("fixture", wrapped, zerolog.Nop(), &bridgeconfig.BridgeConfig{}, &orderingTestMatrix{}, network, commands.NewProcessor)
				bridge.BackgroundCtx = ctx
				if bridgev2.EventHandlingTimeoutTicks != math.MaxInt {
					t.Fatal("connector did not configure serialized handling")
				}
				if name == "framework_default" {
					bridgev2.EventHandlingTimeoutTicks = 10
				}
				if err = bridge.DB.Upgrade(ctx); err != nil {
					t.Fatal(err)
				}
				user, err := bridge.GetUserByMXID(ctx, "@fixture:example.invalid")
				if err != nil {
					t.Fatal(err)
				}
				if err = bridge.DB.UserLogin.Insert(ctx, &database.UserLogin{ID: "fixture", UserMXID: user.MXID}); err != nil {
					t.Fatal(err)
				}
				login, err := bridge.GetExistingUserLoginByID(ctx, "fixture")
				if err != nil || login == nil {
					t.Fatalf("load fixture login: %v", err)
				}
				defer login.BridgeState.Destroy()
				for _, key := range []networkid.PortalKey{{ID: "a"}, {ID: "b"}} {
					portal, err := bridge.GetPortalByKey(ctx, key)
					if err != nil {
						t.Fatal(err)
					}
					// Cache membership before assigning a synthetic room ID; no Matrix
					// implementation or external service is involved in this fixture.
					login.MarkInPortal(ctx, portal)
					portal.MXID = id.RoomID("!" + string(key.ID) + ":example.invalid")
				}
				started := make(chan struct{})
				release := make(chan struct{})
				observed := make(chan string, 3)
				enqueue := func(key networkid.PortalID, handle func()) {
					result := bridge.QueueRemoteEvent(login, orderingTestEvent{key: networkid.PortalKey{ID: key}, handle: handle})
					if !result.Queued {
						t.Fatalf("enqueue: %+v", result)
					}
				}
				enqueue("a", func() { close(started); <-release; observed <- "history" })
				<-started
				enqueue("a", func() { observed <- "later" })
				enqueue("b", func() { observed <- "other" })
				synctest.Wait()
				// Virtual time exercises the actual pinned framework's 30-second
				// ticker and five-minute backgrounding without a wall-clock delay.
				time.Sleep(6 * time.Minute)
				synctest.Wait()
				beforeRelease := []string{<-observed}
				if name == "framework_default" {
					beforeRelease = append(beforeRelease, <-observed)
				}
				if name == "connector_policy" && !slices.Equal(beforeRelease, []string{"other"}) {
					t.Fatalf("later event overtook history: %v", beforeRelease)
				}
				if name == "framework_default" && !slices.Contains(beforeRelease, "later") {
					t.Fatal("fixture did not reproduce framework backgrounding")
				}
				select {
				case extra := <-observed:
					t.Fatalf("unexpected event before history finished: %s", extra)
				default:
				}
				close(release)
				synctest.Wait()
				if got := <-observed; got != "history" {
					t.Fatalf("history completion: %s", got)
				}
				if name == "connector_policy" {
					if got := <-observed; got != "later" {
						t.Fatalf("queued event after history: %s", got)
					}
				}
				cancel()
				synctest.Wait()
			})
		})
	}
}

func TestAsyncPortalEventsAreRejected(t *testing.T) {
	connector := &GMConnector{br: &bridgev2.Bridge{Config: &bridgeconfig.BridgeConfig{AsyncEvents: true}}}
	if err := connector.Start(context.Background()); err == nil || !strings.Contains(err.Error(), "bridge.async_events must be false") {
		t.Fatalf("asynchronous event configuration: %v", err)
	}
}
