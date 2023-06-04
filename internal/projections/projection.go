package projections

import (
	"errors"
	"fmt"

	"github.com/dop251/goja"
	"github.com/ostafen/eventstorm/internal/model"
)

var ErrProjectionExist = errors.New("projection already exists")

type Runtime struct {
	projections map[string]*Context
}

func NewRuntime() *Runtime {
	return &Runtime{
		projections: map[string]*Context{},
	}
}

func (r *Runtime) Register(source, name string) error {
	if _, has := r.projections[name]; has {
		return ErrProjectionExist
	}

	ctx, err := NewContext(source, name)
	if err != nil {
		return err
	}

	r.projections[name] = ctx
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

type Context struct {
	runtime *goja.Runtime
	Options Options
	Output  bool

	selector    SelectorOptions
	partitionBy PartitionFunc
	UpdateFunc  ProjectionFunc
}

func (p *Context) options(opts Options) {
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
	ctx *Context
}

func (w *when) findHandler(handlers map[string]gojaFunc, eventType string) gojaFunc {
	h := handlers[eventType]
	if h == nil {
		return handlers[anyHandler]
	}
	return h
}

type state struct {
	init  bool
	value any
}

type Partitions map[string]*state

func (p Partitions) get(partition string) *state {
	partitionState := p[partition]
	if partitionState == nil {
		partitionState = &state{}
		p[partition] = partitionState
	}
	return partitionState
}

func (w *when) When(handlers map[string]gojaFunc) WhenRes {
	var defaultState state
	var currState *state = &defaultState

	partitions := Partitions{}

	w.ctx.UpdateFunc = w.ctx.UpdateFunc.Chain(func(_ any, e Event) (any, bool) {
		if w.ctx.partitionBy != nil {
			p := w.ctx.partitionBy(e)
			currState = partitions.get(p)
			e.Partition = p
		}

		if !currState.init {
			init := handlers[initFunc]
			currState.value = init.Call(w.ctx.runtime)
			currState.init = true
		}

		handlerFunc := w.findHandler(handlers, e.Type)
		if handlerFunc != nil {
			handlerFunc.Call(w.ctx.runtime, currState.value, e)
		}
		return currState.value, true
	})

	return WhenRes{
		transformBy: transformBy{ctx: w.ctx},
		filterBy:    filterBy{ctx: w.ctx},
		outputTo:    outputTo{ctx: w.ctx},
		outputState: outputState{ctx: w.ctx},
	}
}

type transformBy struct {
	ctx *Context
}

type TransformByRes struct {
	transformBy
	filterBy
	outputState
	outputTo
}

func (t *transformBy) TransformBy(transformFunc gojaFunc) TransformByRes {
	t.ctx.UpdateFunc = t.ctx.UpdateFunc.Chain(func(state any, e Event) (any, bool) {
		out := transformFunc.Call(t.ctx.runtime, state)
		return out, true
	})

	return TransformByRes{
		transformBy: transformBy{ctx: t.ctx},
		filterBy:    filterBy{ctx: t.ctx},
		outputTo:    outputTo{ctx: t.ctx},
		outputState: outputState{ctx: t.ctx},
	}
}

type filterBy struct {
	ctx *Context
}

type FilterByRes struct {
	filterBy
	transformBy
	outputState
	outputTo
}

func (t *filterBy) FilterBy(filterFunc gojaFunc) FilterByRes {
	t.ctx.UpdateFunc = t.ctx.UpdateFunc.Chain(func(state any, e Event) (any, bool) {
		forward, _ := filterFunc.Call(t.ctx.runtime, state).(bool)
		if forward {
			return state, forward
		}
		return nil, forward
	})

	return FilterByRes{
		transformBy: transformBy{ctx: t.ctx},
		filterBy:    filterBy{ctx: t.ctx},
		outputTo:    outputTo{ctx: t.ctx},
		outputState: outputState{ctx: t.ctx},
	}
}

type WhenRes struct {
	transformBy
	filterBy
	outputTo
	outputState
}

type partitionBy struct {
	ctx *Context
}

type PartitionByRes struct {
	when
}

func (p *partitionBy) PartitionBy(partitionFunc gojaFunc) PartitionByRes {
	p.ctx.partitionBy = func(e Event) string {
		partition, _ := partitionFunc.Call(p.ctx.runtime, e).(string)
		return partition
	}
	return PartitionByRes{
		when: when{ctx: p.ctx},
	}
}

type foreachStream struct {
	ctx *Context
}

type ForeachStreamRes struct {
	when
}

func (fe *foreachStream) ForeachStream() ForeachStreamRes {
	fe.ctx.partitionBy = func(e Event) string {
		return e.StreamId
	}

	return ForeachStreamRes{
		when: when{ctx: fe.ctx},
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
	ctx *Context
}

type OutputStateRes struct {
	transformBy
	filterBy
	outputTo
}

// If the projection maintains state, setting this option produces a stream called $projections-{projection-name}-result with the state as the event body.
func (o *outputState) OutputState() OutputStateRes {
	o.ctx.Output = true

	return OutputStateRes{
		transformBy: transformBy{ctx: o.ctx},
		filterBy:    filterBy{ctx: o.ctx},
		outputTo:    outputTo{ctx: o.ctx},
	}
}

type outputTo struct {
	ctx *Context
}

func (o *outputTo) OutputTo(stream string) {
}

func (ctx *Context) fromStreams(stream ...string) FromStreamsRes {
	ctx.selector = SelectorOptions{
		Kind:    SelectorKindStream,
		Streams: stream,
	}

	return FromStreamsRes{
		when:        when{ctx: ctx},
		partitionBy: partitionBy{ctx: ctx},
		outputState: outputState{ctx: ctx},
	}
}

func (ctx *Context) fromStream(stream string) FromStreamsRes {
	return ctx.fromStreams(stream)
}

func (ctx *Context) fromAll() FromAllRes {
	return FromAllRes{
		partitionBy:   partitionBy{ctx: ctx},
		foreachStream: foreachStream{ctx: ctx},
		when:          when{ctx: ctx},
		outputState:   outputState{ctx: ctx},
	}
}

func NewContext(source, name string) (*Context, error) {
	ctx := &Context{
		runtime:    goja.New(),
		UpdateFunc: func(state any, e Event) (any, bool) { return nil, true },
	}
	ctx.setup()

	_, err := ctx.runtime.RunString(source)
	return ctx, err
}

func (ctx *Context) Update(e Event) (any, bool) {
	return ctx.UpdateFunc(nil, e)
}

func panicIfErr(err error) {
	if err != nil {
		panic(err)
	}
}

func (ctx *Context) setupRuntime() {
	ctx.runtime.SetFieldNameMapper(goja.TagFieldNameMapper("json", true))
}

func (ctx *Context) setup() {
	ctx.setupRuntime()
	ctx.setupHandlers()
}

func debug(a ...any) {
	fmt.Println(a...)
}

func (ctx *Context) setupHandlers() {
	err := ctx.runtime.Set("options", ctx.options)
	panicIfErr(err)

	err = ctx.runtime.Set("fromAll", ctx.fromAll)
	panicIfErr(err)

	err = ctx.runtime.Set("fromStream", ctx.fromStream)
	panicIfErr(err)

	err = ctx.runtime.Set("fromStreams", ctx.fromStreams)
	panicIfErr(err)

	err = ctx.runtime.Set("debug", debug)
	panicIfErr(err)
}
