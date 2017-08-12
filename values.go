package pgoutput

import (
	"fmt"

	"github.com/jackc/pgx/pgtype"
)

type RelationSet struct {
	// TODO: Add mutex
	relations map[uint32]Relation
}

func NewRelationSet() *RelationSet {
	return &RelationSet{relations: map[uint32]Relation{}}
}

func (rs *RelationSet) Add(r Relation) {
	rs.relations[r.ID] = r
}

func (rs *RelationSet) Values(id uint32, row []Tuple) (map[string]pgtype.Value, error) {
	values := map[string]pgtype.Value{}
	rel, ok := rs.relations[id]
	if !ok {
		return values, fmt.Errorf("no relation for %d", id)
	}
	// assert same number of row and columns
	for i, tuple := range row {
		col := rel.Columns[i]
		decoder := col.Decoder()
		// TODO: Pass in connection?
		if err := decoder.DecodeText(nil, tuple.Value); err != nil {
			return values, fmt.Errorf("error decoding tuple %d: %s", i, err)
		}
		values[col.Name] = decoder
	}
	return values, nil
}

func (c Column) Decoder() DecoderValue {
	switch c.Type {
	case pgtype.ACLItemArrayOID:
		return &pgtype.ACLItemArray{}
	case pgtype.ACLItemOID:
		return &pgtype.ACLItem{}
	case pgtype.BoolArrayOID:
		return &pgtype.BoolArray{}
	case pgtype.BoolOID:
		return &pgtype.Bool{}
	case pgtype.ByteaArrayOID:
		return &pgtype.BoolArray{}
	case pgtype.ByteaOID:
		return &pgtype.Bytea{}
	case pgtype.CIDOID:
		return &pgtype.CID{}
	case pgtype.CIDRArrayOID:
		return &pgtype.CIDRArray{}
	case pgtype.CIDROID:
		return &pgtype.CIDR{}
	case pgtype.CharOID:
		// Not all possible values of QChar are representable in the text format
		return &pgtype.Unknown{}
	case pgtype.DateArrayOID:
		return &pgtype.DateArray{}
	case pgtype.DateOID:
		return &pgtype.Date{}
	case pgtype.Float4ArrayOID:
		return &pgtype.Float4Array{}
	case pgtype.Float4OID:
		return &pgtype.Float4{}
	case pgtype.Float8ArrayOID:
		return &pgtype.Float8Array{}
	case pgtype.Float8OID:
		return &pgtype.Float8{}
	case pgtype.InetArrayOID:
		return &pgtype.InetArray{}
	case pgtype.InetOID:
		return &pgtype.Inet{}
	case pgtype.Int2ArrayOID:
		return &pgtype.Int2Array{}
	case pgtype.Int2OID:
		return &pgtype.Int2{}
	case pgtype.Int4ArrayOID:
		return &pgtype.Int4Array{}
	case pgtype.Int4OID:
		return &pgtype.Int4{}
	case pgtype.Int8ArrayOID:
		return &pgtype.Int8Array{}
	case pgtype.Int8OID:
		return &pgtype.Int8{}
	case pgtype.JSONBOID:
		return &pgtype.JSONB{}
	case pgtype.JSONOID:
		return &pgtype.JSON{}
	case pgtype.NameOID:
		return &pgtype.Name{}
	case pgtype.OIDOID:
		// pgtype.OID does not implement the value interface
		return &pgtype.Unknown{}
	case pgtype.RecordOID:
		// The text format output format for Records does not include type
		// information and is therefore impossible to decode
		return &pgtype.Unknown{}
	case pgtype.TIDOID:
		return &pgtype.TID{}
	case pgtype.TextArrayOID:
		return &pgtype.TextArray{}
	case pgtype.TextOID:
		return &pgtype.Text{}
	case pgtype.TimestampArrayOID:
		return &pgtype.TimestampArray{}
	case pgtype.TimestampOID:
		return &pgtype.Timestamp{}
	case pgtype.TimestamptzArrayOID:
		return &pgtype.TimestamptzArray{}
	case pgtype.TimestamptzOID:
		return &pgtype.Timestamptz{}
	case pgtype.UUIDOID:
		return &pgtype.UUID{}
	case pgtype.UnknownOID:
		return &pgtype.Unknown{}
	case pgtype.VarcharArrayOID:
		return &pgtype.VarcharArray{}
	case pgtype.VarcharOID:
		return &pgtype.Varchar{}
	case pgtype.XIDOID:
		return &pgtype.XID{}
	default:
		// panic(fmt.Sprintf("unknown OID type %d", c.Type))
		return &pgtype.Unknown{}
	}
}
