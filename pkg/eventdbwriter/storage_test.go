package eventdbwriter

import (
	"context"
	"testing"
	"time"

	_ "github.com/proullon/ramsql/driver"
	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

func TestDBOperations(t *testing.T) {
	storage := createStorage(DBInfo{
		Driver: RamSQL,
	})
	storage.SetYunikornID("yk-1")

	// no result
	events, err := storage.GetAllEventsForApp(context.Background(), "app-1")
	assert.NilError(t, err)
	assert.Equal(t, 0, len(events))

	// add elements
	laterDate := time.Unix(1000, 0)
	baseDate := time.Unix(500, 0)
	err = storage.PersistEvents(context.Background(), 0, []*si.EventRecord{
		{ObjectID: "app-1", Type: si.EventRecord_APP, EventChangeType: si.EventRecord_ADD, EventChangeDetail: si.EventRecord_APP_NEW, TimestampNano: baseDate.UnixNano()},
		{ObjectID: "app-1", Type: si.EventRecord_APP, EventChangeType: si.EventRecord_SET, EventChangeDetail: si.EventRecord_APP_RUNNING, TimestampNano: laterDate.UnixNano()},
		{ObjectID: "app-2", Type: si.EventRecord_APP, EventChangeType: si.EventRecord_SET, EventChangeDetail: si.EventRecord_APP_COMPLETING, TimestampNano: baseDate.UnixNano()},
		{ObjectID: "node-1", Type: si.EventRecord_NODE, EventChangeType: si.EventRecord_ADD, EventChangeDetail: si.EventRecord_NODE_ALLOC, TimestampNano: laterDate.UnixNano()},
	})
	assert.NilError(t, err)

	// check "app-1" events
	events, err = storage.GetAllEventsForApp(context.Background(), "app-1")
	assert.NilError(t, err)
	assert.Equal(t, 2, len(events))
	assert.Equal(t, "app-1", events[0].ObjectID)
	assert.Equal(t, si.EventRecord_APP, events[0].Type)
	assert.Equal(t, si.EventRecord_ADD, events[0].EventChangeType)
	assert.Equal(t, si.EventRecord_APP_NEW, events[0].EventChangeDetail)
	assert.Equal(t, "app-1", events[1].ObjectID)
	assert.Equal(t, si.EventRecord_APP, events[1].Type)
	assert.Equal(t, si.EventRecord_SET, events[1].EventChangeType)
	assert.Equal(t, si.EventRecord_APP_RUNNING, events[1].EventChangeDetail)

	// check "app-2" events
	events, err = storage.GetAllEventsForApp(context.Background(), "app-2")
	assert.NilError(t, err)
	assert.Equal(t, 1, len(events))
	assert.Equal(t, "app-2", events[0].ObjectID)
	assert.Equal(t, si.EventRecord_APP, events[0].Type)
	assert.Equal(t, si.EventRecord_SET, events[0].EventChangeType)
	assert.Equal(t, si.EventRecord_APP_COMPLETING, events[0].EventChangeDetail)

	// remove old entries
	count, err := storage.RemoveObsoleteEntries(context.Background(), baseDate)
	assert.NilError(t, err)
	assert.Equal(t, int64(2), count)
}
