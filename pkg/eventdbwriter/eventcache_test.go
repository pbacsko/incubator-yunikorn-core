package eventdbwriter

import (
	"context"
	"testing"
	"time"

	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

func TestAddSingleEvent(t *testing.T) {
	cache := NewEventCache()

	cache.AddEvent("app-1", &si.EventRecord{TimestampNano: 0})
	cache.AddEvent("app-1", &si.EventRecord{TimestampNano: 1})
	assert.Equal(t, 0, len(cache.fullHistory))
	assert.Equal(t, 0, len(cache.completionTime))
	assert.Equal(t, 0, len(cache.GetEvents("app-1")))

	cache.SetHaveFullHistory("app-1")
	assert.Equal(t, 1, len(cache.fullHistory))
	assert.Equal(t, 0, len(cache.completionTime))
	assert.Equal(t, 2, len(cache.GetEvents("app-1")))
}

func TestAddMultipleEvents(t *testing.T) {
	cache := NewEventCache()

	cache.AddEvents("app-1", []*si.EventRecord{
		{TimestampNano: 0},
		{TimestampNano: 1},
	})
	assert.Equal(t, 0, len(cache.fullHistory))
	assert.Equal(t, 0, len(cache.completionTime))
	assert.Equal(t, 0, len(cache.GetEvents("app-1")))

	cache.SetHaveFullHistory("app-1")
	assert.Equal(t, 1, len(cache.fullHistory))
	assert.Equal(t, 0, len(cache.completionTime))
	assert.Equal(t, 2, len(cache.GetEvents("app-1")))
}

func TestDetectAppCompletion(t *testing.T) {
	cache := NewEventCache()
	cache.AddEvents("app-1", []*si.EventRecord{
		{TimestampNano: 0},
		{TimestampNano: 1, EventChangeDetail: si.EventRecord_APP_COMPLETED},
	})
	assert.Equal(t, 1, len(cache.completionTime))
	_, ok := cache.completionTime["app-1"]
	assert.Assert(t, ok)

	cache = NewEventCache()
	cache.AddEvent("app-1", &si.EventRecord{TimestampNano: 0})
	cache.AddEvent("app-1", &si.EventRecord{TimestampNano: 1, EventChangeDetail: si.EventRecord_APP_COMPLETED})
	assert.Equal(t, 1, len(cache.completionTime))
	_, ok = cache.completionTime["app-1"]
	assert.Assert(t, ok)
}

func TestCompletedAppsCleanup(t *testing.T) {
	cache := NewEventCache()
	cache.expiry = 0
	cache.AddEvents("app-1", []*si.EventRecord{
		{TimestampNano: 0},
		{TimestampNano: 1, EventChangeDetail: si.EventRecord_APP_COMPLETED},
	})
	assert.Equal(t, 1, len(cache.completionTime))

	cache.AddEvents("app-2", []*si.EventRecord{
		{TimestampNano: 0},
		{TimestampNano: 1, EventChangeDetail: si.EventRecord_APP_COMPLETED},
	})
	assert.Equal(t, 2, len(cache.completionTime))

	cnt := cache.cleanUpOldEntries()
	assert.Equal(t, 2, cnt)
	assert.Equal(t, 0, len(cache.fullHistory))
	assert.Equal(t, 0, len(cache.completionTime))
	assert.Equal(t, 0, len(cache.events))
}

func TestEventCacheBackground(t *testing.T) {
	cache := NewEventCache()
	cache.expiry = 10 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	cache.Start(ctx)
	cache.AddEvent("app-1", &si.EventRecord{TimestampNano: 0})
	cache.SetHaveFullHistory("app-1")

	// check that item is still in the cache
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, 1, len(cache.GetEvents("app-1")))

	// add completion event & check removal
	cache.AddEvent("app-1", &si.EventRecord{TimestampNano: 1,
		EventChangeDetail: si.EventRecord_APP_COMPLETED})
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, 0, len(cache.GetEvents("app-1")))

	// check cancellation
	cancel()
	cache.SetHaveFullHistory("app-2")
	cache.AddEvent("app-2", &si.EventRecord{TimestampNano: 0})
	cache.AddEvent("app-2", &si.EventRecord{TimestampNano: 1,
		EventChangeDetail: si.EventRecord_APP_COMPLETED})
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, 2, len(cache.GetEvents("app-2")))
}
