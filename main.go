package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"reflect"
	"time"

	"github.com/jackc/pgx"
	"github.com/pkg/errors"
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

var byteSlice = reflect.TypeOf([]byte(nil))

type Decodable interface {
	Decode(binary.ByteOrder, *bytes.Buffer)
}

func (d *decoder) value(v reflect.Value) {
	fmt.Println(d.buf.Bytes())
	if v.CanAddr() {
		if u, ok := v.Addr().Interface().(Decodable); ok {
			u.Decode(d.order, d.buf)
			return
		}
	}
	switch v.Kind() {
	case reflect.Struct:
		l := v.NumField()
		for i := 0; i < l; i++ {
			// Note: Calling v.CanSet() below is an optimization.
			// It would be sufficient to check the field name,
			// but creating the StructField info for each field is
			// costly (run "go test -bench=ReadStruct" and compare
			// results when making changes to this code).
			fmt.Println(v.Type().Field(i).Name)
			if v := v.Field(i); v.CanSet() {
				d.value(v)
			}
		}
	case reflect.Slice:
		size := int(d.uint16())
		fmt.Println("slice", size)
		v.Set(reflect.MakeSlice(v.Type(), size, size))
		for i := 0; i < v.Len(); i++ {
			d.value(v.Index(i))
		}
	case reflect.Bool:
		fmt.Println(v.Type())
		v.SetBool(d.bool())
	case reflect.String:
		s, err := d.buf.ReadBytes(0)
		if err != nil {
			// TODO: Return an error
			panic(err)
		}
		v.SetString(string(s[:len(s)-1]))
	case reflect.Int8:
		v.SetInt(int64(d.int8()))
	case reflect.Int16:
		v.SetInt(int64(d.int16()))
	case reflect.Int32:
		v.SetInt(int64(d.int32()))
	case reflect.Int64:
		v.SetInt(d.int64())
	case reflect.Uint8:
		v.SetUint(uint64(d.uint8()))
	case reflect.Uint16:
		v.SetUint(uint64(d.uint16()))
	case reflect.Uint32:
		v.SetUint(uint64(d.uint32()))
	case reflect.Uint64:
		v.SetUint(d.uint64())
	}
}

type Timestamp time.Time

func (t *Timestamp) Decode(order binary.ByteOrder, src *bytes.Buffer) {
	micro := int(order.Uint64(src.Next(8)))
	ts := time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)
	*t = Timestamp(ts.Add(time.Duration(micro) * time.Microsecond))
	fmt.Println(time.Time(*t).String())
}

type RowInfo string

func (t *RowInfo) Decode(order binary.ByteOrder, src *bytes.Buffer) {
	switch src.Next(1)[0] {
	case 'O':
		fmt.Println("OLD")
		*t = "OLD"
	case 'K':
		fmt.Println("KEY")
		*t = "KEY"
	default:
		src.UnreadByte()
	}
}

type Begin struct {
	// The final LSN of the transaction.
	LSN uint64
	// Commit timestamp of the transaction. The value is in number of
	// microseconds since PostgreSQL epoch (2000-01-01).
	Timestamp Timestamp
	// 	Xid of the transaction.
	XID int32
}

type Commit struct {
	// The final LSN of the transaction.
	LSN uint64
	// The final LSN of the transaction.
	TransactionLSN uint64
	Timestamp      Timestamp
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
	Old    RowInfo
	Key    RowInfo
	OldRow []Tuple

	New    bool
	NewRow []Tuple
}

type Delete struct {
	/// ID of the relation corresponding to the ID in the relation message.
	RelationID uint32
	// Identifies the following TupleData message as a new tuple.
	KeyOrOld bool // TODO
	Row      []Tuple
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

func (t *Tuple) Decode(order binary.ByteOrder, src *bytes.Buffer) {
	switch src.Next(1)[0] {
	case 'n':
	case 'u':
	case 't':
		size := int(order.Uint32(src.Next(4)))
		t.Flag = 't'
		t.Value = src.Next(size)
	}
}

type TupleData struct {
	Tuples []Tuple
}

func parse(src []byte) error {
	msgType := src[0]
	var msg interface{}
	switch msgType {
	case 'B':
		msg = &Begin{}
	case 'C':
		msg = &Commit{}
	case 'O':
		msg = &Origin{}
	case 'R':
		msg = &Relation{}
	case 'I':
		msg = &Insert{}
	case 'U':
		msg = &Update{}
	case 'D':
		msg = &Delete{}
	default:
		return errors.Errorf("unknown message type: %c", msgType)
	}
	v := reflect.Indirect(reflect.ValueOf(msg))
	d := &decoder{order: binary.BigEndian, buf: bytes.NewBuffer(src[1:])}
	d.value(v)
	fmt.Printf("%#v\n", msg)
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
