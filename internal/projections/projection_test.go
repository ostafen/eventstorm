package projections

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/suite"
)

type ProjectionSuite struct {
	suite.Suite
}

func TestProjectionSuite(t *testing.T) {
	suite.Run(t, &ProjectionSuite{})
}

func (s *ProjectionSuite) TestOptionsFunc() {
	ctx, err := NewContext(`
		options({
			resultStreamName: "test_projection_result",
			$includeLinks:    true,
			reorderEvents:    false,
			processingLag:    10
		})
	`, "test")
	s.NoError(err)

	s.Equal(ctx.Options, Options{
		ResultStream:  "test_projection_result",
		IncludeLinks:  true,
		ReorderEvents: false,
		ProcessingLag: 10,
	})
	s.False(ctx.Output)
}

func (s *ProjectionSuite) TestFromStreamSelector() {
	ctx, err := NewContext(`
		fromStream('test-stream')
	`, "test")
	s.NoError(err)

	s.Equal(ctx.selector, SelectorOptions{
		Kind:    SelectorKindStream,
		Streams: []string{"test-stream"},
	})
}

func (s *ProjectionSuite) TestOutputState() {
	ctx, err := NewContext(`
		fromStream('test-stream')
			.outputState()
	`, "test")
	s.NoError(err)

	s.True(ctx.Output)
}

func (s *ProjectionSuite) TestFromStreamWhen() {
	ctx, err := NewContext(`
		fromStream('test-stream')
			.when({
				$init: function() {
					return {count: 0}
				},
				testEvent: function(state, event) {
					state.count += 1
				}
			})
	`, "test")
	s.NoError(err)

	state, _ := ctx.Update(Event{
		Type: "invalid-type",
	})
	s.Equal(map[string]any{
		"count": int64(0),
	}, state)

	for i := 0; i < 100; i++ {
		state, _ = ctx.Update(Event{
			Type: "testEvent",
		})
		s.Equal(map[string]any{
			"count": int64(i + 1),
		}, state)
	}
}

func (s *ProjectionSuite) TestTransformBy() {
	ctx, err := NewContext(`
		fromStream('test-stream')
			.when({
				$init: function() {
					return {count: 0}
				},
				testEvent: function(state, event) {
					state.count += 1
				}
			})
			.transformBy(function(state) {
				return {...state, extraField: 'extra-field'}
			})
	`, "test")
	s.NoError(err)

	for i := 0; i < 100; i++ {
		state, forward := ctx.Update(Event{
			Type: "testEvent",
		})
		s.True(forward)
		s.Equal(map[string]any{
			"count":      int64(i + 1),
			"extraField": "extra-field",
		}, state)
	}
}

func (s *ProjectionSuite) TestFilterBy() {
	ctx, err := NewContext(`
		fromStream('test-stream')
			.when({
				$init: function() {
					return {count: 0}
				},
				testEvent: function(state, event) {
					state.count += 1
				}
			})
			.filterBy(function(state) {
				return state.count > 50
			})
			.transformBy(function(state) {
				return {...state, extraField: 'extra-field'}
			})
	`, "test")
	s.NoError(err)

	for i := 0; i < 50; i++ {
		state, forward := ctx.Update(Event{
			Type: "testEvent",
		})
		s.False(forward)
		s.Nil(state)
	}

	for i := 0; i < 50; i++ {
		state, forward := ctx.Update(Event{
			Type: "testEvent",
		})
		s.True(forward)
		s.Equal(map[string]any{
			"count":      int64(51 + i),
			"extraField": "extra-field",
		}, state)
	}
}

func (s *ProjectionSuite) TestPartitionBy() {
	ctx, err := NewContext(`
		fromStream('test-stream')
			.partitionBy(function(e) {
				return e.eventType
			})
			.when({
				$init: function() {
					return { count: 0 }
				},
				$any: function(state, event) {
					state.count += 1
					state.partition = event.partition
				}
			})
	`, "test")
	s.NoError(err)

	for i := 0; i < 100; i++ {
		p := fmt.Sprintf("p-%d", i/10)

		state, forward := ctx.Update(Event{
			Type: p,
		})
		s.True(forward)
		s.Equal(map[string]any{
			"count":     int64(i%10 + 1),
			"partition": p,
		}, state)
	}
}

func (s *ProjectionSuite) TestAnyEventHandler() {
	ctx, err := NewContext(`
		fromStream('test-stream')
			.when({
				$init: function() {
					return { count: 0, anyCount: 0 }
				},
				$any: function(state, event) {
					state.anyCount += 1
				},
				testEvent: function(state, event) {
					state.count += 1
				}
			})
	`, "test")
	s.NoError(err)

	// specific event selector takes precedence
	state, forward := ctx.Update(Event{
		Type: "testEvent",
	})
	s.True(forward)
	s.Equal(state, map[string]any{
		"count":    int64(1),
		"anyCount": int64(0),
	})

	state, forward = ctx.Update(Event{
		Type: "testEvent1",
	})
	s.True(forward)
	s.Equal(state, map[string]any{
		"count":    int64(1),
		"anyCount": int64(1),
	})
}
