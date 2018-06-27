package pgoutput

import (
	"context"
	"fmt"
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

type Handler func(Message) error

func NewSubscription(name, publication string) *Subscription {
	return &Subscription{
		Name:          name,
		Publication:   publication,
		WaitTimeout:   time.Second * 10,
		StatusTimeout: time.Second * 10,
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

func (s *Subscription) Start(ctx context.Context, conn *pgx.ReplicationConn, h Handler) (err error) {
	err = conn.StartReplication(s.Name, 0, -1, pluginArgs("1", s.Publication))
	if err != nil {
		return fmt.Errorf("failed to start replication: %s", err)
	}

	var maxWal uint64

	sendStatus := func() error {
		k, err := pgx.NewStandbyStatus(maxWal)
		if err != nil {
			return fmt.Errorf("error creating standby status: %s", err)
		}

		if err := conn.SendStandbyStatus(k); err != nil {
			return fmt.Errorf("failed to send standy status: %s", err)
		}

		return nil
	}

	tick := time.NewTicker(s.StatusTimeout)
	defer tick.Stop()

	for {
		select {
		case <-tick.C:
			if err = sendStatus(); err != nil {
				return
			}

		case <-ctx.Done():
			return

		default:
			var message *pgx.ReplicationMessage
			wctx, cancel := context.WithTimeout(ctx, s.WaitTimeout)
			message, err = conn.WaitForReplicationMessage(wctx)
			cancel()

			if err == context.DeadlineExceeded {
				continue
			}

			if err == context.Canceled {
				return nil
			}

			if err != nil {
				return fmt.Errorf("replication failed: %s", err)
			}

			if message.WalMessage != nil {
				if message.WalMessage.WalStart > maxWal {
					maxWal = message.WalMessage.WalStart
				}

				logmsg, err := Parse(message.WalMessage.WalData)
				if err != nil {
					return fmt.Errorf("invalid pgoutput message: %s", err)
				}

				if err = h(logmsg); err != nil {
					return fmt.Errorf("error handling WAL data: %s", err)
				}
			} else if message.ServerHeartbeat != nil {
				if message.ServerHeartbeat.ReplyRequested == 1 {
					if err = sendStatus(); err != nil {
						return
					}
				}
			}
		}
	}
}
