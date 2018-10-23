package main

import (
	"context"
	"fmt"
	"log"

	"github.com/jackc/pgx"
	"github.com/kyleconroy/pgoutput"
)

func main() {
	ctx := context.Background()
	config := pgx.ConnConfig{Database: "opsdash", User: "replicant"}
	conn, err := pgx.ReplicationConnect(config)
	if err != nil {
		log.Fatal(err)
	}

	set := pgoutput.NewRelationSet(nil)

	dump := func(relation uint32, row []pgoutput.Tuple) error {
		values, err := set.Values(relation, row)
		if err != nil {
			return fmt.Errorf("error parsing values: %s", err)
		}
		for name, value := range values {
			val := value.Get()
			log.Printf("%s (%T): %#v", name, val, val)
		}
		return nil
	}

	handler := func(m pgoutput.Message, walPos uint64) error {
		switch v := m.(type) {
		case pgoutput.Relation:
			log.Printf("RELATION")
			set.Add(v)
		case pgoutput.Insert:
			log.Printf("INSERT")
			return dump(v.RelationID, v.Row)
		case pgoutput.Update:
			log.Printf("UPDATE")
			return dump(v.RelationID, v.Row)
		case pgoutput.Delete:
			log.Printf("DELETE")
			return dump(v.RelationID, v.Row)
		}
		return nil
	}

	sub := pgoutput.NewSubscription(conn, "sub1", "pub1", 0, false)
	if err := sub.Start(ctx, 0, handler); err != nil {
		log.Fatal(err)
	}
}
