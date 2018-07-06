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

	conn   *pgx.ReplicationConn
	maxWal uint64

	// Mutex is used to prevent reading and writing to a connection at the same time
	statusMtx sync.Mutex
}

type Handler func(Message, uint64) error

func NewSubscription(conn *pgx.ReplicationConn, name, publication string) *Subscription {
	return &Subscription{
		Name:          name,
		Publication:   publication,
		WaitTimeout:   1 * time.Second,
		StatusTimeout: 10 * time.Second,
		CopyData:      true,

		conn: conn,
	}
}

func pluginArgs(version, publication string) string {
	return fmt.Sprintf(`"proto_version" '%s', "publication_names" '%s'`, version, publication)
}

// CreateSlot creates a replication slot if it doesn't exist
func (s *Subscription) CreateSlot() (err error) {
	// If creating the replication slot fails with code 42710, this means
	// the replication slot already exists.
	if err = s.conn.CreateReplicationSlot(s.Name, "pgoutput"); err != nil {
		pgerr, ok := err.(pgx.PgError)
		if !ok || pgerr.Code != "42710" {
			return
		}

		err = nil
	}

	return
}

func (s *Subscription) sendStatus(wal uint64, flush bool) error {
	s.statusMtx.Lock()
	defer s.statusMtx.Unlock()

	var vals []uint64
	if flush {
		vals = []uint64{wal}
	} else {
		vals = []uint64{0, 0, wal}
	}

	k, err := pgx.NewStandbyStatus(vals...)
	if err != nil {
		return fmt.Errorf("error creating status: %s", err)
	}

	if err = s.conn.SendStandbyStatus(k); err != nil {
		return err
	}

	return nil
}

// Flush send the status message to server indicating that we've fully applied all of the events until maxWal.
// This allows PostgreSQL to purge it's WAL logs
func (s *Subscription) Flush() error {
	return s.sendStatus(atomic.LoadUint64(&s.maxWal), true)
}

// Start starts replication and block until error or ctx is canceled
func (s *Subscription) Start(ctx context.Context, startLSN uint64, h Handler) (err error) {
	err = s.conn.StartReplication(s.Name, startLSN, -1, pluginArgs("1", s.Publication))
	if err != nil {
		return fmt.Errorf("failed to start replication: %s", err)
	}

	s.maxWal = startLSN

	go func() {
		tick := time.NewTicker(s.StatusTimeout)
		defer tick.Stop()

		for {
			select {
			case <-tick.C:
				if err = s.sendStatus(atomic.LoadUint64(&s.maxWal), false); err != nil {
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
			if err = s.sendStatus(atomic.LoadUint64(&s.maxWal), false); err != nil {
				return fmt.Errorf("Unable to send final status: %s", err)
			}

			return

		default:
			var message *pgx.ReplicationMessage
			wctx, cancel := context.WithTimeout(ctx, s.WaitTimeout)
			s.statusMtx.Lock()
			message, err = s.conn.WaitForReplicationMessage(wctx)
			s.statusMtx.Unlock()
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

				if walStart > atomic.LoadUint64(&s.maxWal) {
					atomic.StoreUint64(&s.maxWal, walStart)
				}

				logmsg, err := Parse(message.WalMessage.WalData)
				if err != nil {
					return fmt.Errorf("invalid pgoutput message: %s", err)
				}

				// Ignore the error from handler for now
				h(logmsg, walStart)
			} else if message.ServerHeartbeat != nil {
				if message.ServerHeartbeat.ReplyRequested == 1 {
					if err = s.sendStatus(atomic.LoadUint64(&s.maxWal), false); err != nil {
						return
					}
				}
			} else {
				return fmt.Errorf("No WalMessage/ServerHeartbeat defined in packet, should not happen")
			}
		}
	}
}
