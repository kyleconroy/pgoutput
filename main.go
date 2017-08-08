package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"log"

	"github.com/jackc/pgx"
	"github.com/pkg/errors"
)

type Begin struct {
	// The final LSN of the transaction.
	LSN uint64
	// Commit timestamp of the transaction. The value is in number of
	// microseconds since PostgreSQL epoch (2000-01-01).
	Timestamp uint64
	// 	Xid of the transaction.
	XID int32
}

type Insert struct {
	/// ID of the relation corresponding to the ID in the relation message.
	RelationID int
	// Identifies the following TupleData message as a new tuple.
	TupleData TupleData
}

type Column struct {
	Key  bool
	Name string
	Mode int
	Type int
}

type Relation struct {
	ID        int
	Namespace string
	Name      string
	Replica   int
	Columns   []Column
}

type Tuple struct {
	Value string
}

type TupleData struct {
	Tuples []Tuple
}

func parse(src []byte) error {
	msgType := src[0]
	buf := bytes.NewBuffer(src[1:])

	switch msgType {
	case 'B':
		b := Begin{}
		if err := binary.Read(buf, binary.BigEndian, &b); err != nil {
			return fmt.Errorf("cloud not parse begin message: %s", err)
		}
		// ts := time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)
		// b.Date = ts.Add(time.Duration(micro) * time.Microsecond)
		fmt.Printf("%#v\n", b)
	case 'C':
		fmt.Println("Commit")
	case 'O':
		fmt.Println("Origin")
	case 'R':
		r := Relation{}
		r.ID = int(binary.BigEndian.Uint32(buf.Next(4)))
		ns, err := buf.ReadBytes(0)
		if err != nil {
			return fmt.Errorf("error on ns: %s", err)
		}
		r.Namespace = string(ns[:len(ns)-1])
		name, err := buf.ReadBytes(0)
		if err != nil {
			return fmt.Errorf("error on name: %s", err)
		}
		r.Name = string(name[:len(name)-1])
		r.Replica = int(buf.Next(1)[0])

		columns := int(binary.BigEndian.Uint16(buf.Next(2)))
		r.Columns = make([]Column, columns)

		for i := 0; i < columns; i++ {
			// Flags for the column. Currently can be either 0 for no flags or 1 which marks the column as part of the key.
			flags := int(buf.Next(1)[0])
			col, err := buf.ReadBytes(0)
			if err != nil {
				return fmt.Errorf("error on col: %s", err)
			}
			r.Columns[i] = Column{
				Key:  flags > 0,
				Name: string(col[:len(col)-1]),
				Type: int(binary.BigEndian.Uint32(buf.Next(4))),
				Mode: int(binary.BigEndian.Uint32(buf.Next(4))),
			}
		}
		fmt.Printf("%#v\n", r)
	case 'I':
		in := Insert{}
		in.RelationID = int(binary.BigEndian.Uint32(buf.Next(4)))
		if td := buf.Next(1); td[0] != 'N' {
			return fmt.Errorf("Expected Insert message to have new tuple set")
		}
		columns := int(binary.BigEndian.Uint16(buf.Next(2)))
		td := TupleData{}
		td.Tuples = make([]Tuple, columns)
		in.TupleData = td
		for i := 0; i < columns; i++ {
			columnType := buf.Next(1)
			switch columnType[0] {
			case 'n':
			case 'u':
			case 't':
				columnLength := int(binary.BigEndian.Uint32(buf.Next(4)))
				td.Tuples[i] = Tuple{string(buf.Next(columnLength))}
			}
		}
		fmt.Printf("%#v\n", in)
	case 'U':
		fmt.Println("Update")
	case 'D':
		fmt.Println("Delete")
	default:
		return errors.Errorf("unknown message type: %c", msgType)
	}
	return nil
}

func main() {
	config := pgx.ConnConfig{
		Database: "opsdash",
		User:     "replicant",
	}

	conn, err := pgx.ReplicationConnect(config)
	if err != nil {
		log.Fatal(err)
	}

	err = conn.StartReplication("sub1", 0, -1, `("proto_version" '1', "publication_names" 'pub1')`)
	if err != nil {
		log.Fatalf("Failed to start replication: %v", err)
	}

	ctx := context.Background()

	for {
		var message *pgx.ReplicationMessage

		message, err = conn.WaitForReplicationMessage(ctx)
		if err != nil {
			log.Fatalf("Replication failed: %v %s", err)
		}

		if message.WalMessage != nil {
			// The waldata payload with the test_decoding plugin looks like:
			// public.replication_test: INSERT: a[integer]:2
			// What we wanna do here is check that once we find one of our inserted times,
			// that they occur in the wal stream in the order we executed them.
			parse(message.WalMessage.WalData)
		}
		if message.ServerHeartbeat != nil {
			log.Printf("Got heartbeat: %s", message.ServerHeartbeat)
		}
	}
}
