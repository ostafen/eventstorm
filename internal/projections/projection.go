package projections

import (
	"context"
	"errors"
	"fmt"

	"github.com/dop251/goja"
	"github.com/ostafen/eventstorm/internal/model"
	"github.com/ostafen/eventstorm/internal/streams"
)

var ErrProjectionExist = errors.New("projection already exists")

type subscribeFunc func(context.Context, model.ReadOptions) (*streams.Subscription, error)

type Runtime struct {
	subscribe   subscribeFunc
	projections map[string]*Projection
}

func NewRuntime(subscribe subscribeFunc) *Runtime {
	return &Runtime{
		subscribe:   subscribe,
		projections: map[string]*Projection{},
	}
}

func (r *Runtime) Register(source, name string) error {
	if _, has := r.projections[name]; has {
		return ErrProjectionExist
	}

	p, err := CompileProjection(source, name)
	if err != nil {
		return err
	}

	r.projections[name] = p
	return nil
}

const (
	initFunc   = "$init"
	anyHandler = "$any"
)

type Options struct {
	ResultStream  string `json:"resultStreamName"`
	IncludeLinks  bool   `json:"$includeLinks"`
	ReorderEvents bool   `json:"reorderEvents"`
	ProcessingLag int    `json:"processingLag"`
}

type Event struct {
	IsJson          bool              `json:"isJson"`
	Data            map[string]any    `json:"data"`
	Body            map[string]any    `json:"body"`
	BodyRaw         string            `json:"bodyRaw"`
	SequenceNumber  int64             `json:"sequenceNumber"`
	MetadataRaw     map[string]string `json:"metadataRaw"`
	LinkMetadataRaw string            `json:"linkMetadataRaw"`
	Partition       string            `json:"partition"`
	Type            string            `json:"eventType"`
	StreamId        string            `json:"streamId"`
}

func NewEvent(e model.Event) Event {
	return Event{
		IsJson:         e.IsJson(),
		Data:           nil, // ???
		Body:           e.Json(),
		BodyRaw:        string(e.Data),
		SequenceNumber: -1, // ???
		MetadataRaw:    e.Metadata,
		Partition:      "",
		Type:           e.Metadata.EventType(),
		StreamId:       e.StreamIdentifier,
	}
}

type ProjectionFunc func(state any, e Event) (any, bool)

func (f ProjectionFunc) Chain(g ProjectionFunc) ProjectionFunc {
	return func(state any, e Event) (any, bool) {
		s, forward := f(state, e)
		if forward {
			return g(s, e)
		}
		return s, false
	}
}

type PartitionFunc func(e Event) string

type SelectorKind int8

const (
	SelectorKindStream SelectorKind = iota
	SelectorKindAll
)

type SelectorOptions struct {
	Kind    SelectorKind
	Streams []string
}

func (s *SelectorOptions) Matches(e *model.Event) bool {
	if s.Kind == SelectorKindAll {
		return true
	}

	for _, s := range s.Streams {
		if s == e.StreamIdentifier {
			return true
		}
	}
	return false
}

type Projection struct {
	runtime *goja.Runtime

	defaultState    any
	partitionsState map[string]any

	Name    string
	Options Options
	Output  bool

	selector    SelectorOptions
	partitionBy PartitionFunc
	updateFunc  ProjectionFunc
}

func (p *Projection) ResultStream() string {
	if p.Options.ResultStream != "" {
		return p.Options.ResultStream
	}
	return fmt.Sprintf("$projections-%s-result", p.Name)
}

func (p *Projection) options(opts Options) {
	p.Options = opts
}

type gojaFunc func(goja.FunctionCall) goja.Value

func (f gojaFunc) Call(vm *goja.Runtime, values ...any) any {
	params := make([]goja.Value, 0, len(values))
	for _, v := range values {
		params = append(params, vm.ToValue(v))
	}

	out := f(goja.FunctionCall{
		Arguments: params,
	})

	var x any
	vm.ExportTo(out, &x)
	return x
}

type when struct {
	p *Projection
}

func (w *when) findHandler(handlers map[string]gojaFunc, eventType string) gojaFunc {
	h := handlers[eventType]
	if h == nil {
		return handlers[anyHandler]
	}
	return h
}

func (w *when) When(handlers map[string]gojaFunc) WhenRes {
	w.p.updateFunc = w.p.updateFunc.Chain(func(state any, e Event) (any, bool) {
		if state == nil {
			initFunc := handlers[initFunc]
			state = initFunc.Call(w.p.runtime)
		}

		handlerFunc := w.findHandler(handlers, e.Type)
		if handlerFunc != nil {
			handlerFunc.Call(w.p.runtime, state, e)
		}
		return state, true
	})

	return WhenRes{
		transformBy: transformBy{p: w.p},
		filterBy:    filterBy{p: w.p},
		outputTo:    outputTo{p: w.p},
		outputState: outputState{p: w.p},
	}
}

type transformBy struct {
	p *Projection
}

type TransformByRes struct {
	transformBy
	filterBy
	outputState
	outputTo
}

func (t *transformBy) TransformBy(transformFunc gojaFunc) TransformByRes {
	t.p.updateFunc = t.p.updateFunc.Chain(func(state any, e Event) (any, bool) {
		out := transformFunc.Call(t.p.runtime, state)
		return out, true
	})

	return TransformByRes{
		transformBy: transformBy{p: t.p},
		filterBy:    filterBy{p: t.p},
		outputTo:    outputTo{p: t.p},
		outputState: outputState{p: t.p},
	}
}

type filterBy struct {
	p *Projection
}

type FilterByRes struct {
	filterBy
	transformBy
	outputState
	outputTo
}

func (t *filterBy) FilterBy(filterFunc gojaFunc) FilterByRes {
	t.p.updateFunc = t.p.updateFunc.Chain(func(state any, e Event) (any, bool) {
		forward, _ := filterFunc.Call(t.p.runtime, state).(bool)
		return state, forward
	})

	return FilterByRes{
		transformBy: transformBy{p: t.p},
		filterBy:    filterBy{p: t.p},
		outputTo:    outputTo{p: t.p},
		outputState: outputState{p: t.p},
	}
}

type WhenRes struct {
	transformBy
	filterBy
	outputTo
	outputState
}

type partitionBy struct {
	p *Projection
}

type PartitionByRes struct {
	when
}

func (p *partitionBy) PartitionBy(partitionFunc gojaFunc) PartitionByRes {
	p.p.partitionsState = map[string]any{}
	p.p.partitionBy = func(e Event) string {
		partition, _ := partitionFunc.Call(p.p.runtime, e).(string)
		return partition
	}
	return PartitionByRes{
		when: when{p: p.p},
	}
}

type foreachStream struct {
	p *Projection
}

type ForeachStreamRes struct {
	when
}

func (fe *foreachStream) ForeachStream() ForeachStreamRes {
	fe.p.partitionBy = func(e Event) string {
		return e.StreamId
	}

	return ForeachStreamRes{
		when: when{p: fe.p},
	}
}

type FromStreamsRes struct {
	partitionBy
	when
	outputState
}

type FromAllRes struct {
	partitionBy
	foreachStream
	when
	outputState
}

type FromStreamsMatchingRes struct {
	when
}

type outputState struct {
	p *Projection
}

type OutputStateRes struct {
	transformBy
	filterBy
	outputTo
}

// If the projection maintains state, setting this option produces a stream called $projections-{projection-name}-result with the state as the event body.
func (o *outputState) OutputState() OutputStateRes {
	o.p.Output = true

	return OutputStateRes{
		transformBy: transformBy{p: o.p},
		filterBy:    filterBy{p: o.p},
		outputTo:    outputTo{p: o.p},
	}
}

type outputTo struct {
	p *Projection
}

func (o *outputTo) OutputTo(stream string) {
}

func (p *Projection) fromStreams(stream ...string) FromStreamsRes {
	p.selector = SelectorOptions{
		Kind:    SelectorKindStream,
		Streams: stream,
	}

	return FromStreamsRes{
		when:        when{p: p},
		partitionBy: partitionBy{p: p},
		outputState: outputState{p: p},
	}
}

func (p *Projection) fromStream(stream string) FromStreamsRes {
	return p.fromStreams(stream)
}

func (p *Projection) fromAll() FromAllRes {
	return FromAllRes{
		partitionBy:   partitionBy{p: p},
		foreachStream: foreachStream{p: p},
		when:          when{p: p},
		outputState:   outputState{p: p},
	}
}

func CompileProjection(source, name string) (*Projection, error) {
	p := &Projection{
		runtime:    goja.New(),
		Name:       name,
		updateFunc: func(state any, e Event) (any, bool) { return state, true },
	}
	p.setup()

	_, err := p.runtime.RunString(source)
	return p, err
}

func (p *Projection) SetState(state any) {
	p.defaultState = state
}

func (p *Projection) SetPartitionState(partition string, state any) {
	if p.partitionsState == nil {
		p.partitionsState = make(map[string]any)
	}
	p.partitionsState[partition] = state
}

func (p *Projection) getState(e Event) (any, string) {
	if !p.IsPartitioned() {
		return p.defaultState, ""
	}

	partition := p.partitionBy(e)
	return p.partitionsState[partition], partition
}

func (p *Projection) IsPartitioned() bool {
	return p.partitionBy != nil
}

func (p *Projection) Update(e Event) any {
	state, partition := p.getState(e)

	e.Partition = partition
	newState, forward := p.updateFunc(state, e)
	if p.IsPartitioned() {
		p.partitionsState[partition] = newState
	} else {
		p.defaultState = newState
	}

	if !forward {
		return nil
	}
	return newState
}

func panicIfErr(err error) {
	if err != nil {
		panic(err)
	}
}

func (p *Projection) setupRuntime() {
	p.runtime.SetFieldNameMapper(goja.TagFieldNameMapper("json", true))
}

func (p *Projection) setup() {
	p.setupRuntime()
	p.setupHandlers()
}

func log(a ...any) {
	fmt.Println(a...)
}

func (p *Projection) setupHandlers() {
	err := p.runtime.Set("options", p.options)
	panicIfErr(err)

	err = p.runtime.Set("fromAll", p.fromAll)
	panicIfErr(err)

	err = p.runtime.Set("fromStream", p.fromStream)
	panicIfErr(err)

	err = p.runtime.Set("fromStreams", p.fromStreams)
	panicIfErr(err)

	err = p.runtime.Set("log", log)
	panicIfErr(err)
}
