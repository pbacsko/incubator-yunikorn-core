package eventdbwriter

import (
	"encoding/json"
	"testing"
	"time"

	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-core/pkg/common/resources"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

func TestEntryFromSI(t *testing.T) {
	res := resources.NewResourceFromMap(map[string]resources.Quantity{
		"cpu":    1,
		"memory": 100,
	})
	event := &si.EventRecord{
		Type:              si.EventRecord_APP,
		ObjectID:          "alloc-1",
		ReferenceID:       "app-1",
		Message:           "message",
		TimestampNano:     123,
		EventChangeType:   si.EventRecord_SET,
		EventChangeDetail: si.EventRecord_APP_NEW,
		Resource:          res.ToProto(),
	}
	dbEntry := entryFromSI("yunikornuuid", 100, event)
	assert.Equal(t, int32(si.EventRecord_APP), dbEntry.Type)
	assert.Equal(t, "yunikornuuid", dbEntry.YunikornID)
	assert.Equal(t, uint64(100), dbEntry.EventID)
	assert.Equal(t, "alloc-1", dbEntry.ObjectID)
	assert.Equal(t, "app-1", dbEntry.ReferenceID)
	assert.Equal(t, "message", dbEntry.Message)
	assert.Equal(t, int64(123), dbEntry.Timestamp.UnixNano())
	assert.Equal(t, int32(si.EventRecord_SET), dbEntry.ChangeType)
	assert.Equal(t, int32(si.EventRecord_APP_NEW), dbEntry.ChangeDetail)
	assert.Assert(t, dbEntry.Resource != "")
	var unmarshalledRes si.Resource
	err := json.Unmarshal([]byte(dbEntry.Resource), &unmarshalledRes)
	assert.NilError(t, err)
	assert.Equal(t, 2, len(unmarshalledRes.Resources))
	assert.Equal(t, int64(1), unmarshalledRes.Resources["cpu"].Value)
	assert.Equal(t, int64(100), unmarshalledRes.Resources["memory"].Value)
}

func TestSIfromEntry(t *testing.T) {
	dbEntry := EventDBEntry{
		YunikornID:   "yunikorn-100",
		Type:         3, // EventRecord_NODE
		ObjectID:     "node-1",
		ReferenceID:  "alloc-1",
		Message:      "message",
		Timestamp:    time.Unix(0, 1234),
		ChangeType:   int32(2),   // EventRecord_ADD
		ChangeDetail: int32(303), // EventRecord_NODE_ALLOC
		Resource:     "{\"resources\":{\"cpu\":{\"value\":1},\"memory\":{\"value\":100}}}",
	}

	event := siFromEntry(dbEntry)
	assert.Equal(t, si.EventRecord_NODE, event.Type)
	assert.Equal(t, "node-1", event.ObjectID)
	assert.Equal(t, "message", event.Message)
	assert.Equal(t, "alloc-1", event.ReferenceID)
	assert.Equal(t, int64(1234), event.TimestampNano)
	assert.Equal(t, si.EventRecord_ADD, event.EventChangeType)
	assert.Equal(t, si.EventRecord_NODE_ALLOC, event.EventChangeDetail)
	assert.Equal(t, 2, len(event.Resource.Resources))
	assert.Equal(t, int64(1), event.Resource.Resources["cpu"].Value)
	assert.Equal(t, int64(100), event.Resource.Resources["memory"].Value)

	dbEntry.Resource = "xyz"
	event = siFromEntry(dbEntry)
	assert.Assert(t, event.Resource == nil)
}
