package projections

import (
	"fmt"
	"log"
	"math/rand"
	"testing"
)

func TestXxx(t *testing.T) {
	ctx := NewContext()

	_, err := ctx.runtime.RunString(`
	options({
		resultStreamName: "my_demo_projection_result",
		$includeLinks:    true,
		reorderEvents:    false,
		processingLag:    0
	})
	
	fromAll('myStream')
	.foreachStream()
	.when({
        $init: function () {
            return {
                count: 0
            }
        },
        $any: function (state, event) {
            state.count += 1
        },
    })
	.filterBy(function(state) {
		return state.count >= 5
	})
	.transformBy(function(state) {
		return {Total: state.count, Transformed: 'transformed'}
	})
	.transformBy(function(state) {
		return {...state, newField: Math.random()}
	})
	.outputState()
	`)
	panicIfErr(err)

	for i := 0; i < 1000; i++ {
		partition := rand.Intn(10)

		res := ctx.Update(Event{
			StreamId: fmt.Sprintf("stream-%d", partition),
		})

		log.Println(res)
	}

	log.Println(ctx.selector.Streams)

}
