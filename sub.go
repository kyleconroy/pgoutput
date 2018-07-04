package pgoutput

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx"
)

type Subscription struct {
	Name          string
	Publication   string
	WaitTimeout   time.Duration
	StatusTimeout time.Duration
	CopyData      bool
}

type Handler func(Message, uint64) error

func NewSubscription(name, publication string) *Subscription {
	return &Subscription{
		Name:          name,
		Publication:   publication,
		WaitTimeout:   10 * time.Second,
		StatusTimeout: 10 * time.Second,
		CopyData:      true,
	}
}

func pluginArgs(version, publication string) string {
	return fmt.Sprintf(`"proto_version" '%s', "publication_names" '%s'`, version, publication)
}

func (s *Subscription) CreateSlot(conn *pgx.ReplicationConn) (err error) {
	// If creating the replication slot fails with code 42710, this means
	// the replication slot already exists.
	if err = conn.CreateReplicationSlot(s.Name, "pgoutput"); err != nil {
		pgerr, ok := err.(pgx.PgError)
		if !ok || pgerr.Code != "42710" {
			return
		}

		err = nil
	}

	return
}

func (s *Subscription) Start(ctx context.Context, conn *pgx.ReplicationConn, startLSN uint64, h Handler) (err error) {
	err = conn.StartReplication(s.Name, startLSN, -1, pluginArgs("1", s.Publication))
	if err != nil {
		return fmt.Errorf("failed to start replication: %s", err)
	}

	maxWal := startLSN

	// Mutex is used to prevent reading and writing to a connection at the same time
	var statusMtx sync.Mutex
	sendStatus := func(wal uint64) error {
		k, err := pgx.NewStandbyStatus(wal)
		if err != nil {
			return fmt.Errorf("error creating status: %s", err)
		}

		statusMtx.Lock()
		defer statusMtx.Unlock()
		if err := conn.SendStandbyStatus(k); err != nil {
			return err
		}

		return nil
	}

	go func() {
		tick := time.NewTicker(s.StatusTimeout)
		defer tick.Stop()

		for {
			select {
			case <-tick.C:
				if err = sendStatus(atomic.LoadUint64(&maxWal)); err != nil {
					return
				}

			case <-ctx.Done():
				return
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			// Send final status
			if err = sendStatus(atomic.LoadUint64(&maxWal)); err != nil {
				return fmt.Errorf("Unable to send final status: %s", err)
			}

			return

		default:
			var message *pgx.ReplicationMessage
			wctx, cancel := context.WithTimeout(ctx, s.WaitTimeout)
			statusMtx.Lock()
			message, err = conn.WaitForReplicationMessage(wctx)
			statusMtx.Unlock()
			cancel()

			if err == context.DeadlineExceeded {
				continue
			} else if err == context.Canceled {
				return nil
			} else if err != nil {
				return fmt.Errorf("replication failed: %s", err)
			}

			if message.WalMessage != nil {
				walStart := message.WalMessage.WalStart
				// Skip stuff that's in past
				if walStart > 0 && walStart <= startLSN {
					continue
				}

				if walStart > atomic.LoadUint64(&maxWal) {
					atomic.StoreUint64(&maxWal, walStart)
				}

				logmsg, err := Parse(message.WalMessage.WalData)
				if err != nil {
					return fmt.Errorf("invalid pgoutput message: %s", err)
				}

				// Ignore the error from handler for now
				h(logmsg, walStart)
			} else if message.ServerHeartbeat != nil {
				if message.ServerHeartbeat.ReplyRequested == 1 {
					if err = sendStatus(atomic.LoadUint64(&maxWal)); err != nil {
						return
					}
				}
			} else {
				return fmt.Errorf("No WalMessage/ServerHeartbeat defined in packet, should not happen")
			}
		}
	}
}
