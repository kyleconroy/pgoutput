package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx"
)

type decoder struct {
	order binary.ByteOrder
	buf   *bytes.Buffer
}

func (d *decoder) bool() bool {
	x := d.buf.Next(1)[0]
	return x != 0

}

func (d *decoder) uint8() uint8 {
	x := d.buf.Next(1)[0]
	return x

}

func (d *decoder) uint16() uint16 {
	x := d.order.Uint16(d.buf.Next(2))
	return x
}

func (d *decoder) string() string {
	s, err := d.buf.ReadBytes(0)
	if err != nil {
		// TODO: Return an error
		panic(err)
	}
	return string(s[:len(s)-1])
}

func (d *decoder) uint32() uint32 {
	x := d.order.Uint32(d.buf.Next(4))
	return x

}

func (d *decoder) uint64() uint64 {
	x := d.order.Uint64(d.buf.Next(8))
	return x
}

func (d *decoder) int8() int8   { return int8(d.uint8()) }
func (d *decoder) int16() int16 { return int16(d.uint16()) }
func (d *decoder) int32() int32 { return int32(d.uint32()) }
func (d *decoder) int64() int64 { return int64(d.uint64()) }

func (d *decoder) timestamp() time.Time {
	micro := int(d.uint64())
	ts := time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)
	return ts.Add(time.Duration(micro) * time.Microsecond)
}

func (d *decoder) rowinfo(char byte) bool {
	if d.buf.Next(1)[0] == char {
		return true
	} else {
		d.buf.UnreadByte()
		return false
	}
}

func (d *decoder) tupledata() []Tuple {
	size := int(d.uint16())
	data := make([]Tuple, size)
	for i := 0; i < size; i++ {
		switch d.buf.Next(1)[0] {
		case 'n':
		case 'u':
		case 't':
			vsize := int(d.order.Uint32(d.buf.Next(4)))
			data[i] = Tuple{Flag: 't', Value: d.buf.Next(vsize)}
		}
	}
	return data
}

func (d *decoder) columns() []Column {
	size := int(d.uint16())
	data := make([]Column, size)
	for i := 0; i < size; i++ {
		data[i] = Column{
			Key:  d.bool(),
			Name: d.string(),
			Mode: d.uint32(),
			Type: d.uint32(),
		}
	}
	return data
}

type Begin struct {
	// The final LSN of the transaction.
	LSN uint64
	// Commit timestamp of the transaction. The value is in number of
	// microseconds since PostgreSQL epoch (2000-01-01).
	Timestamp time.Time
	// 	Xid of the transaction.
	XID int32
}

type Commit struct {
	Flags uint8
	// The final LSN of the transaction.
	LSN uint64
	// The final LSN of the transaction.
	TransactionLSN uint64
	Timestamp      time.Time
}

type Relation struct {
	// ID of the relation.
	ID uint32
	// Namespace (empty string for pg_catalog).
	Namespace string
	Name      string
	Replica   uint8
	Columns   []Column
}

type Insert struct {
	/// ID of the relation corresponding to the ID in the relation message.
	RelationID uint32
	// Identifies the following TupleData message as a new tuple.
	New bool
	Row []Tuple
}

type Update struct {
	/// ID of the relation corresponding to the ID in the relation message.
	RelationID uint32
	// Identifies the following TupleData message as a new tuple.
	Old    bool
	Key    bool
	New    bool
	OldRow []Tuple
	Row    []Tuple
}

type Delete struct {
	/// ID of the relation corresponding to the ID in the relation message.
	RelationID uint32
	// Identifies the following TupleData message as a new tuple.
	Key bool // TODO
	Old bool // TODO
	Row []Tuple
}

type Origin struct {
	LSN  uint64
	Name string
}

type Column struct {
	Key  bool
	Name string
	Mode uint32
	Type uint32
}

type Tuple struct {
	Flag  int8
	Value []byte
}

func parse(src []byte) interface{} {
	msgType := src[0]
	d := &decoder{order: binary.BigEndian, buf: bytes.NewBuffer(src[1:])}
	switch msgType {
	case 'B':
		b := Begin{}
		b.LSN = d.uint64()
		b.Timestamp = d.timestamp()
		b.XID = d.int32()
		return b
	case 'C':
		c := Commit{}
		c.Flags = d.uint8()
		c.LSN = d.uint64()
		c.TransactionLSN = d.uint64()
		c.Timestamp = d.timestamp()
		return c
	case 'O':
		o := Origin{}
		o.LSN = d.uint64()
		o.Name = d.string()
		return o
	case 'R':
		r := Relation{}
		r.ID = d.uint32()
		r.Namespace = d.string()
		r.Name = d.string()
		r.Replica = d.uint8()
		r.Columns = d.columns()
		return r
	case 'I':
		i := Insert{}
		i.RelationID = d.uint32()
		i.New = d.uint8() > 0
		i.Row = d.tupledata()
		return i
	case 'U':
		u := Update{}
		u.RelationID = d.uint32()
		u.Key = d.rowinfo('K')
		u.Old = d.rowinfo('O')
		if u.Key || u.Old {
			u.OldRow = d.tupledata()
		}
		u.New = d.uint8() > 0
		u.Row = d.tupledata()
		return u
	case 'D':
		dl := Delete{}
		dl.RelationID = d.uint32()
		dl.Key = d.rowinfo('K')
		dl.Old = d.rowinfo('O')
		dl.Row = d.tupledata()
		return dl
	default:
		return nil
	}
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
			switch m := parse(message.WalMessage.WalData).(type) {
			case Insert:
				for _, tuple := range m.Row {
					fmt.Println(string(tuple.Value))
				}
			case Update:
				for _, tuple := range m.Row {
					fmt.Println(string(tuple.Value))
				}
			case Delete:
				for _, tuple := range m.Row {
					fmt.Println(string(tuple.Value))
				}
			}
		}
		if message.ServerHeartbeat != nil {
			log.Printf("Got heartbeat: %s", message.ServerHeartbeat)
		}
	}
}
