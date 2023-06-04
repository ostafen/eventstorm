package projections

import (
	"fmt"
	"testing"

	"github.com/ostafen/eventstorm/internal/model"
	"github.com/stretchr/testify/suite"
)

type ProjectionSuite struct {
	suite.Suite
}

func TestProjectionSuite(t *testing.T) {
	suite.Run(t, &ProjectionSuite{})
}

func (s *ProjectionSuite) TestOptionsFunc() {
	p, err := NewProjection(`
		options({
			$includeLinks:    true,
			reorderEvents:    false,
			processingLag:    10
		})
	`, "test")
	s.NoError(err)

	s.Equal(p.Options, Options{
		ResultStream:  "",
		IncludeLinks:  true,
		ReorderEvents: false,
		ProcessingLag: 10,
	})
	s.Equal("$projections-test-result", p.ResultStream())
	s.False(p.Output)

	p, err = NewProjection(`
		options({
			resultStreamName: "test_projection_result",
			$includeLinks:    true,
			reorderEvents:    false,
			processingLag:    10
		})
	`, "test")
	s.NoError(err)

	s.Equal(p.Options, Options{
		ResultStream:  "test_projection_result",
		IncludeLinks:  true,
		ReorderEvents: false,
		ProcessingLag: 10,
	})
	s.Equal("test_projection_result", p.ResultStream())
	s.False(p.Output)
}

func (s *ProjectionSuite) TestFromStreamSelector() {
	p, err := NewProjection(`
		fromStream('test-stream')
	`, "test")
	s.NoError(err)

	s.Equal(p.selector, SelectorOptions{
		Kind:    SelectorKindStream,
		Streams: []string{"test-stream"},
	})

	s.False(p.selector.Matches(&model.Event{}))

	s.True(p.selector.Matches(&model.Event{
		StreamIdentifier: "test-stream",
	}))
}

func (s *ProjectionSuite) TestOutputState() {
	p, err := NewProjection(`
		fromStream('test-stream')
			.outputState()
	`, "test")
	s.NoError(err)

	s.True(p.Output)
}

func (s *ProjectionSuite) TestFromStreamWhen() {
	p, err := NewProjection(`
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

	state, _ := p.Update(Event{
		Type: "invalid-type",
	})
	s.Equal(map[string]any{
		"count": int64(0),
	}, state)

	for i := 0; i < 100; i++ {
		state, _ = p.Update(Event{
			Type: "testEvent",
		})
		s.Equal(map[string]any{
			"count": int64(i + 1),
		}, state)
	}
}

func (s *ProjectionSuite) TestTransformBy() {
	p, err := NewProjection(`
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
		state, forward := p.Update(Event{
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
	p, err := NewProjection(`
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
		state, forward := p.Update(Event{
			Type: "testEvent",
		})
		s.False(forward)
		s.Nil(state)
	}

	for i := 0; i < 50; i++ {
		state, forward := p.Update(Event{
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
	proj, err := NewProjection(`
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

		state, forward := proj.Update(Event{
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
	p, err := NewProjection(`
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
	state, forward := p.Update(Event{
		Type: "testEvent",
	})
	s.True(forward)
	s.Equal(state, map[string]any{
		"count":    int64(1),
		"anyCount": int64(0),
	})

	state, forward = p.Update(Event{
		Type: "testEvent1",
	})
	s.True(forward)
	s.Equal(state, map[string]any{
		"count":    int64(1),
		"anyCount": int64(1),
	})
}
