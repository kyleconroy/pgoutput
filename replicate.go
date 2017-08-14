package pgoutput

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx"
)

type LogicalReplication struct {
	Subscription  string
	Publication   string
	WaitTimeout   time.Duration
	StatusTimeout time.Duration
	Handler       func(Message) error
}

func pluginArgs(version, publication string) string {
	return fmt.Sprintf(`("proto_version" '%s', "publication_names" '%s')`, version, publication)
}

func (lr *LogicalReplication) Start(ctx context.Context, conn *pgx.ReplicationConn) error {
	// TODO: Struct Validation here
	err := conn.StartReplication(lr.Subscription, 0, -1, pluginArgs("1", lr.Publication))
	if err != nil {
		return fmt.Errorf("Failed to start replication: %v", err)
	}

	var maxWal uint64
	tick := time.NewTicker(lr.StatusTimeout).C
	for {
		select {
		case <-tick:
			log.Println("pub status")
			if maxWal == 0 {
				continue
			}
			k, err := pgx.NewStandbyStatus(maxWal)
			if err != nil {
				return fmt.Errorf("Create new status failed: %s", err)
			}
			if err := conn.SendStandbyStatus(k); err != nil {
				return fmt.Errorf("Sending standy status failed: %s", err)
			}
		default:
			var message *pgx.ReplicationMessage
			wctx, cancel := context.WithTimeout(ctx, lr.WaitTimeout)
			message, err = conn.WaitForReplicationMessage(wctx)
			cancel()
			if err == context.DeadlineExceeded {
				continue
			}
			if err != nil {
				return fmt.Errorf("Replication failed: %s", err)
			}
			if message.WalMessage == nil {
				log.Println("nil wal message")
				continue
			}
			if message.WalMessage.WalStart > maxWal {
				maxWal = message.WalMessage.WalStart
			}
			logmsg, err := Parse(message.WalMessage.WalData)
			if err != nil {
				return fmt.Errorf("invalid pgoutput message: %s", err)
			}
			if err := lr.Handler(logmsg); err != nil {
				return fmt.Errorf("error handling waldata: %s", err)
			}
		}
	}
}
