package streams

import (
	"github.com/ostafen/eventstorm/internal/model"
)

func (r *ReadReq) AsOptions() (model.ReadOptions, error) {
	opts := r.Options

	var count int64
	switch o := opts.CountOption.(type) {
	case *ReadReq_Options_Count:
		count = int64(o.Count)
	case *ReadReq_Options_Subscription:
		count = -1
	}

	streamOpts, allOpts := opts.parseReadOpts()
	return model.ReadOptions{
		AllOptions:    allOpts,
		StreamOptions: streamOpts,
		Direction:     readDirection(opts.ReadDirection),
		Count:         count,
		ResolveLinks:  opts.ResolveLinks,
	}, nil
}

func (opts *ReadReq_Options) parseReadOpts() (*model.StreamOptions, *model.AllOptions) {
	var streamOpts *model.StreamOptions
	var allOpts *model.AllOptions

	switch o := opts.StreamOption.(type) {
	case *ReadReq_Options_Stream:
		streamOpts = &model.StreamOptions{
			Identifier: string(o.Stream.StreamIdentifier.StreamName),
		}

		switch ro := o.Stream.RevisionOption.(type) {
		case *ReadReq_Options_StreamOptions_Start:
			streamOpts.RevisionKind = model.RevisionReadKindStart
		case *ReadReq_Options_StreamOptions_End:
			streamOpts.RevisionKind = model.RevisionReadKindEnd
		case *ReadReq_Options_StreamOptions_Revision:
			streamOpts.RevisionKind = model.RevisionReadKindRevision
			streamOpts.Revision = ro.Revision
		}
	case *ReadReq_Options_All:
		switch all := o.All.AllOption.(type) {
		case *ReadReq_Options_AllOptions_Position:
			allOpts = &model.AllOptions{
				Kind:            model.ReadAllKindPosition,
				PreparePosition: all.Position.PreparePosition,
				CommitPosition:  all.Position.CommitPosition,
			}
		case *ReadReq_Options_AllOptions_Start:
			allOpts = &model.AllOptions{Kind: model.ReadAllKindStart}
		case *ReadReq_Options_AllOptions_End:
			allOpts = &model.AllOptions{Kind: model.ReadAllKindEnd}
		}
	}

	if allOpts != nil {
		filterOpts := model.FilterOptions{}
		switch o := opts.FilterOption.(type) {
		case *ReadReq_Options_Filter:
			switch f := o.Filter.Filter.(type) {
			case *ReadReq_Options_FilterOptions_StreamIdentifier:
				filterOpts.Kind = model.FilterKindStreamIdentifier
				filterOpts.Expr = f.StreamIdentifier.ToExpr()
			case *ReadReq_Options_FilterOptions_EventType:
				filterOpts.Kind = model.FilterKindEventType
				filterOpts.Expr = f.EventType.ToExpr()
			}
		case *ReadReq_Options_NoFilter:
			filterOpts.Kind = model.FilterKindNoFilter
		}

		allOpts.Filter = filterOpts
	}

	return streamOpts, allOpts
}

func (e *ReadReq_Options_FilterOptions_Expression) ToExpr() model.FilterExpression {
	return model.FilterExpression{
		Regex:  e.Regex,
		Prefix: e.Prefix,
	}
}

func readDirection(direction ReadReq_Options_ReadDirection) model.Direction {
	switch direction {
	case ReadReq_Options_Forwards:
		return model.DirectionForwards
	}
	return model.DirectionBackwards
}
