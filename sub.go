package pgoutput

import (
	"context"
	"fmt"
	"log"
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
	return fmt.Sprintf(`("proto_version" '%s', "publication_names" '%s')`, version, publication)
}

func (s *Subscription) Start(ctx context.Context, conn *pgx.ReplicationConn, h Handler) error {
	// TODO: Struct Validation here
	_ = conn.DropReplicationSlot(s.Name)

	// If creating the replication slot fails with code 42710, this means
	// the replication slot already exists.
	err := conn.CreateReplicationSlot(s.Name, "pgoutput")
	if err != nil {
		pgerr, ok := err.(pgx.PgError)
		if !ok {
			return fmt.Errorf("failed to create replication slot: %s", err)
		}
		if pgerr.Code != "42710" {
			return fmt.Errorf("failed to create replication slot: %s", err)
		}
	}

	// rows, err := conn.IdentifySystem()
	// if err != nil {
	// 		return err
	// }

	// var slotName, consitentPoint, snapshotName, outputPlugin string
	// if err := row.Scan(&slotName, &consitentPoint, &snapshotName, &outputPlugin); err != nil {
	// 	return err
	// }

	// log.Printf("slotName: %s\n", slotName)
	// log.Printf("consitentPoint: %s\n", consitentPoint)
	// log.Printf("snapshotName: %s\n", snapshotName)
	// log.Printf("outputPlugin: %s\n", outputPlugin)

	// Open a transaction on the server
	// SET TRANSACTION SNAPSHOT id
	// read all the data from the tables

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

	tick := time.NewTicker(s.StatusTimeout).C
	for {
		select {
		case <-tick:
			log.Println("pub status")
			if maxWal == 0 {
				continue
			}
			if err := sendStatus(); err != nil {
				return err
			}
		default:
			var message *pgx.ReplicationMessage
			wctx, cancel := context.WithTimeout(ctx, s.WaitTimeout)
			message, err = conn.WaitForReplicationMessage(wctx)
			cancel()
			if err == context.DeadlineExceeded {
				continue
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
				if err := h(logmsg); err != nil {
					return fmt.Errorf("error handling waldata: %s", err)
				}
			}
			if message.ServerHeartbeat != nil {
				if message.ServerHeartbeat.ReplyRequested == 1 {
					log.Println("server wants a reply")
					if err := sendStatus(); err != nil {
						return err
					}
				}
			}
		}
	}
}
