package pgoutput

import (
	"context"
	"fmt"
	"log"
	"testing"

	"google.golang.org/api/option"

	"cloud.google.com/go/bigquery"

	"github.com/jackc/pgx"
	"github.com/jackc/pgx/pgtype"
)

type rowSaver struct {
	rel Relation
	row []Tuple
}

type typeDecoder interface {
	pgtype.TextDecoder
	pgtype.Value
}

func infer(rel Relation) bigquery.Schema {
	schema := []*bigquery.FieldSchema{}
	for _, col := range rel.Columns {
		switch col.Type {
		case pgtype.TextOID:
			schema = append(schema, &bigquery.FieldSchema{
				Name: col.Name,
				Type: bigquery.StringFieldType,
			})
		case pgtype.Int4OID:
			schema = append(schema, &bigquery.FieldSchema{
				Name: col.Name,
				Type: bigquery.IntegerFieldType,
			})
		default:
		}
	}
	return bigquery.Schema(schema)
}

func (rs *rowSaver) Save() (map[string]bigquery.Value, string, error) {
	values := map[string]bigquery.Value{}
	for i, tuple := range rs.row {
		col := rs.rel.Columns[i]
		var decoder typeDecoder
		switch col.Type {
		case pgtype.TextOID:
			decoder = &pgtype.Text{}
		case pgtype.Int4OID:
			decoder = &pgtype.Int4{}
		default:
			panic("don't know type")
		}
		decoder.DecodeText(nil, tuple.Value)
		values[col.Name] = bigquery.Value(decoder.Get())
	}
	return values, "", nil
}

func TestLogicalReplication(t *testing.T) {
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

	client, err := bigquery.NewClient(ctx, "pgdataflow", option.WithCredentialsFile("pgdataflow-662555ce817d.json"))
	if err != nil {
		t.Fatal(err)
	}
	d := client.Dataset("my_dataset")
	ta := d.Table("foo")
	u := ta.Uploader()

	var rel Relation

	for {
		var message *pgx.ReplicationMessage

		message, err = conn.WaitForReplicationMessage(ctx)
		if err != nil {
			log.Fatalf("Replication failed: %v %s", err)
		}

		if message.WalMessage != nil {
			// The waldata payload with the test_decoding plugin looks like:
			m, err := Parse(message.WalMessage.WalData)
			if err != nil {
				log.Fatal(err)
			}

			switch v := m.(type) {
			case Relation:
				rel = v
				// if err := ta.Create(ctx, infer(rel)); err != nil {
				// 	t.Fatal(err)
				// }
			case Insert:
				fmt.Println("insert")
				rs := rowSaver{rel, v.Row}
				// Schema is inferred from the score type.
				if err := u.Put(ctx, &rs); err != nil {
					if pme, ok := err.(bigquery.PutMultiError); ok {
						errs := []bigquery.RowInsertionError(pme)
						t.Log(errs)
					}
					t.Fatalf("%#v", err)
				}
			case Update:
				for _, tuple := range v.Row {
					fmt.Println(string(tuple.Value))
				}
			case Delete:
				for _, tuple := range v.Row {
					fmt.Println(string(tuple.Value))
				}
			}
		}
		if message.ServerHeartbeat != nil {
			log.Printf("Got heartbeat: %s", message.ServerHeartbeat)
		}
	}
}
