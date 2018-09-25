package pgoutput

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"path/filepath"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/jackc/pgx"
)

func GenerateLogicalReplicationFiles(t *testing.T) {
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
	count := 0

	for {
		var message *pgx.ReplicationMessage

		message, err = conn.WaitForReplicationMessage(ctx)
		if err != nil {
			log.Fatalf("Replication failed: %v %s", message, err)
		}

		if message.WalMessage != nil {
			ioutil.WriteFile(fmt.Sprintf("%03d.waldata", count), message.WalMessage.WalData, 0644)
			count += 1
		}
		if message.ServerHeartbeat != nil {
			log.Printf("Got heartbeat: %s", message.ServerHeartbeat)
		}
	}
}

func TestParseWalData(t *testing.T) {
	files, _ := filepath.Glob("testdata/*")
	set := NewRelationSet()

	expected := map[int]struct {
		ID  int32
		Val string
	}{
		2:  {40, "forty"},
		11: {11, "eleven"},
		14: {12, "twelve"},
	}

	for i, file := range files {
		waldata, _ := ioutil.ReadFile(file)
		m, err := Parse(waldata)
		if err != nil {
			t.Errorf("error parsing %s: %s", file, err)
			continue
		}

		switch v := m.(type) {
		case Relation:
			set.Add(v)
		case Insert:
			t.Run(fmt.Sprintf("waldata/%d", i), func(t *testing.T) {
				values, err := set.Values(v.RelationID, v.Row)
				if err != nil {
					t.Error(err)
				}

				exp := expected[i]
				if diff := cmp.Diff(exp.ID, values["id"].Get()); diff != "" {
					t.Errorf("id: %s", diff)
				}
				if diff := cmp.Diff(exp.Val, values["val"].Get()); diff != "" {
					t.Errorf("val: %s", diff)
				}
			})
		case Type:
			if v.ID != 35756 {
				t.Errorf("Type OID: %d", v.ID)
			}
			if v.Namespace != "public" {
				t.Errorf("Type namespace: %s", v.Namespace)
			}
			if v.Name != "ticket_state" {
				t.Errorf("Type name: %s", v.Name)
			}
		}
	}
}
