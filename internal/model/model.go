package model

import (
	"database/sql/driver"
	"encoding/json"
)

type Metadata map[string]string

func (m Metadata) ContentType() string {
	return m[ContentTypeMetadata]
}

func (m Metadata) SetContentType(eventType string) {
	m[ContentTypeMetadata] = eventType
}

func (m Metadata) EventType() string {
	return m[EventTypeMetadata]
}

func (m Metadata) SetEventType(eventType string) {
	m[EventTypeMetadata] = eventType
}

type Event struct {
	UUID             string
	StreamIdentifier string
	Metadata         Metadata
	CustomMetadata   []byte
	Data             []byte
	StreamRevision   uint64
	GlobalPosition   uint64
}

const (
	ContentTypeMetadata = "content-type"
	EventTypeMetadata   = "type"
	ContentTypeJson     = "application/json"
)

func (e *Event) IsJson() bool {
	return e.Metadata[ContentTypeMetadata] == ContentTypeJson
}

func (e *Event) Json() map[string]any {
	if !e.IsJson() {
		panic("not a json body")
	}
	var body map[string]any
	json.Unmarshal(e.Data, &body)
	return body
}

func (meta Metadata) Value() (driver.Value, error) {
	return json.Marshal(meta)
}

func (meta *Metadata) Scan(value interface{}) error {
	b, ok := value.([]byte)
	if ok {
		return json.Unmarshal(b, meta)
	}
	return nil
}

type RevisionAppendKind int8

const (
	RevisionAppendKindAny = iota
	RevisionAppendKindNoStream
	RevisionAppendKindExist
	RevisionAppendKindRevision
)

type AppendOptions struct {
	Revision uint64
	Kind     RevisionAppendKind
}

type EventStream interface {
	Next() (*Event, error)
}

type Direction int8

const (
	DirectionForwards = iota
	DirectionBackwards
)

type ReadOptions struct {
	Direction     Direction
	Count         int64
	ResolveLinks  bool
	AllOptions    *AllOptions
	StreamOptions *StreamOptions
}

func (opts *ReadOptions) IsSubscription() bool {
	return opts.Count < 0
}

type FilterKind int8

const (
	FilterKindNoFilter = iota
	FilterKindEventType
	FilterKindStreamIdentifier
)

type FilterOptions struct {
	Expr FilterExpression
	Kind FilterKind
}

type FilterExpression struct {
	Regex  string
	Prefix []string
}

type StreamOptions struct {
	Identifier   string
	RevisionKind RevisionReadKind
	Revision     uint64
}

type ReadAllKind int8

const (
	ReadAllKindStart = iota
	ReadAllKindEnd
	ReadAllKindPosition
)

type AllOptions struct {
	Filter          FilterOptions
	Kind            ReadAllKind
	PreparePosition uint64
	CommitPosition  uint64
}

type RevisionReadKind int8

const (
	RevisionReadKindStart = iota
	RevisionReadKindEnd
	RevisionReadKindRevision
)

type AppendResult struct {
	Revision        uint64
	PreparePosition uint64
	CommitPosition  uint64
}
