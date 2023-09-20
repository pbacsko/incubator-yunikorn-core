package eventdbwriter

import (
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

	expiry = 0
	defer func() {
		expiry = 15 * time.Minute
	}()
	cnt := cache.cleanUpOldEntries()
	assert.Equal(t, 2, cnt)
	assert.Equal(t, 0, len(cache.fullHistory))
	assert.Equal(t, 0, len(cache.completionTime))
	assert.Equal(t, 0, len(cache.events))
}
