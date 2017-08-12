# pgoutput

```go
package main

import (
	"context"
	"log"

	"github.com/kyleconroy/pgoutput"
	"github.com/jackc/pgx"
)

func main() {
	ctx := context.Background()
	config := pgx.ConnConfig{Database: "db", User: "replicant"}
	conn, err := pgx.ReplicationConnect(config)
	if err != nil {
		log.Fatal(err)
	}

	err = conn.StartReplication("sub1", 0, -1, `("proto_version" '1', "publication_names" 'pub1')`)
	if err != nil {
		log.Fatalf("Failed to start replication: %v", err)
	}

	set := pgoutput.NewRelationSet()
	for {
		var message *pgx.ReplicationMessage
		message, err = conn.WaitForReplicationMessage(ctx)
		if err != nil {
			log.Fatalf("Replication failed: %v %s", err)
		}
		if message.WalMessage == nil {
			continue
		}
		m, err := pgoutput.Parse(message.WalMessage.WalData)
		if err != nil {
			log.Fatalf("error parsing waldata: %s", err)
		}
		switch v := m.(type) {
		case pgoutput.Relation:
			set.Add(v)
		case pgoutput.Insert:
				values, err := set.Values(v.RelationID, v.Row)
				if err != nil {
					log.Fatalf("error parsing values: %s", err)
				}
				for name, value := range values {
					val := value.Get()
					log.Printf("%s\t%T\t%#v", name, val, val)
				}
			})
		}
	}
}
```
