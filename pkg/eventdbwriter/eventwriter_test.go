package eventdbwriter

import (
	"context"
	"testing"
	"time"

	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-core/pkg/webservice/dao"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

func TestSimpleEventPersistence(t *testing.T) {
	mockDB := NewMockDB()
	client := &MockClient{}
	client.response = &dao.EventRecordDAO{
		LowestID:     0,
		HighestID:    1,
		InstanceUUID: "yunikornUUID",
		EventRecords: []*si.EventRecord{
			{TimestampNano: 100, ObjectID: "app-1"},
			{TimestampNano: 200, ObjectID: "app-1"},
		},
	}
	reader := NewEventWriter(mockDB, client, NewEventCache())

	err := reader.fetchAndPersistEvents(context.Background())
	assert.NilError(t, err)
	assert.Equal(t, "yunikornUUID", reader.ykID)

	assert.Equal(t, 1, len(mockDB.getPersistenceCalls()))
	assert.Equal(t, "yunikornUUID", mockDB.getYunikornID())
	call := mockDB.persistenceCalls[0]
	assert.Equal(t, "yunikornUUID", call.yunikornID)
	assert.Equal(t, uint64(0), call.startEventID)
	assert.Equal(t, 2, len(call.events))
	assert.Equal(t, int64(100), call.events[0].TimestampNano)
	assert.Equal(t, int64(200), call.events[1].TimestampNano)
}

func TestPersistMultipleRounds(t *testing.T) {
	mockDB := NewMockDB()
	client := &MockClient{}
	events := []*si.EventRecord{
		{TimestampNano: 100, ObjectID: "app-1"},
		{TimestampNano: 200, ObjectID: "app-1"},
	}
	client.response = &dao.EventRecordDAO{
		LowestID:     0,
		HighestID:    1,
		InstanceUUID: "yunikornUUID",
		EventRecords: events,
	}
	reader := NewEventWriter(mockDB, client, NewEventCache())

	// first round, two events
	err := reader.fetchAndPersistEvents(context.Background())
	assert.NilError(t, err)
	assert.Equal(t, "yunikornUUID", reader.ykID)

	// second round, two new events (4 in total)
	events[0] = &si.EventRecord{
		TimestampNano:     300,
		Type:              si.EventRecord_APP,
		EventChangeType:   si.EventRecord_SET,
		EventChangeDetail: si.EventRecord_APP_RUNNING,
		ObjectID:          "app-1"}
	events[1] = &si.EventRecord{
		TimestampNano:     400,
		Type:              si.EventRecord_NODE,
		EventChangeType:   si.EventRecord_ADD,
		EventChangeDetail: si.EventRecord_NODE_ALLOC,
		ObjectID:          "node-1"}
	client.response.EventRecords = events
	client.response.LowestID = 2
	client.response.HighestID = 3
	err = reader.fetchAndPersistEvents(context.Background())
	assert.NilError(t, err)
	persistenceCalls := mockDB.getPersistenceCalls()
	assert.Equal(t, 2, len(persistenceCalls))
	call := persistenceCalls[1]
	assert.Equal(t, 2, len(call.events))
	assert.Equal(t, "yunikornUUID", call.yunikornID)
	assert.Equal(t, uint64(2), call.startEventID)
	assert.Equal(t, 2, len(call.events))
	assert.Equal(t, int64(300), call.events[0].TimestampNano)
	assert.Equal(t, int64(400), call.events[1].TimestampNano)
	assert.Equal(t, uint64(4), reader.startID)

	// third round, no new events
	client.response.EventRecords = nil
	client.response.LowestID = 0
	client.response.HighestID = 3
	err = reader.fetchAndPersistEvents(context.Background())
	assert.NilError(t, err)
	assert.Equal(t, 2, len(mockDB.getPersistenceCalls()))

	// fourth round, one new event
	events = make([]*si.EventRecord, 1)
	events[0] = &si.EventRecord{
		TimestampNano:     500,
		Type:              si.EventRecord_APP,
		EventChangeType:   si.EventRecord_SET,
		EventChangeDetail: si.EventRecord_APP_COMPLETING,
		ObjectID:          "app-1"}
	client.response.LowestID = 4
	client.response.HighestID = 4
	client.response.EventRecords = events

	err = reader.fetchAndPersistEvents(context.Background())
	assert.NilError(t, err)
	persistenceCalls = mockDB.getPersistenceCalls()
	assert.Equal(t, 3, len(persistenceCalls))
	call = persistenceCalls[2]
	assert.Equal(t, 1, len(call.events))
	assert.Equal(t, "yunikornUUID", call.yunikornID)
	assert.Equal(t, uint64(4), call.startEventID)
	assert.Equal(t, 1, len(call.events))
	assert.Equal(t, int64(500), call.events[0].TimestampNano)
}

func TestDetectYunikornRestart(t *testing.T) {
	mockDB := NewMockDB()
	client := &MockClient{}
	cache := NewEventCache()
	events := []*si.EventRecord{
		{TimestampNano: 100, Type: si.EventRecord_APP, ObjectID: "app-1"},
		{TimestampNano: 200, Type: si.EventRecord_APP, ObjectID: "app-1"},
	}
	client.response = &dao.EventRecordDAO{
		LowestID:     0,
		HighestID:    1,
		InstanceUUID: "yunikornUUID",
		EventRecords: events,
	}
	reader := NewEventWriter(mockDB, client, cache)

	newEvents := []*si.EventRecord{
		{TimestampNano: 11, Type: si.EventRecord_APP,
			EventChangeType:   si.EventRecord_ADD,
			EventChangeDetail: si.EventRecord_DETAILS_NONE,
			ObjectID:          "app-1"},
		{TimestampNano: 22, Type: si.EventRecord_APP, ObjectID: "app-1"},
	}
	err := reader.fetchAndPersistEvents(context.Background())
	assert.NilError(t, err)

	// next response with a different InstanceUUID
	client.response = &dao.EventRecordDAO{
		LowestID:     0,
		HighestID:    1,
		InstanceUUID: "yunikornUUID-2",
		EventRecords: newEvents,
	}
	err = reader.fetchAndPersistEvents(context.Background())
	assert.NilError(t, err)
	assert.Equal(t, 1, len(cache.events))
	assert.Equal(t, "yunikornUUID-2", reader.ykID)
	assert.Equal(t, "yunikornUUID-2", mockDB.getYunikornID())
	call := mockDB.getPersistenceCalls()[1]
	assert.Equal(t, 2, len(call.events))
	assert.Equal(t, "yunikornUUID-2", call.yunikornID)
	assert.Equal(t, uint64(0), call.startEventID)
	assert.Equal(t, 2, len(call.events))
	assert.Equal(t, int64(11), call.events[0].TimestampNano)
	assert.Equal(t, int64(22), call.events[1].TimestampNano)
}

func TestClientFailure(t *testing.T) {
	mockDB := NewMockDB()
	cache := NewEventCache()
	client := &MockClient{}
	events := []*si.EventRecord{
		{TimestampNano: 100, Type: si.EventRecord_APP, ObjectID: "app-1"},
		{TimestampNano: 200, Type: si.EventRecord_APP, ObjectID: "app-1"},
	}
	client.response = &dao.EventRecordDAO{
		LowestID:     0,
		HighestID:    1,
		InstanceUUID: "yunikornUUID",
		EventRecords: events,
	}
	reader := NewEventWriter(mockDB, client, cache)
	client.setFailure(true)

	err := reader.fetchAndPersistEvents(context.Background())
	assert.ErrorContains(t, err, "error while getting events")
	assert.Equal(t, 0, len(mockDB.getPersistenceCalls()))
	assert.Equal(t, 0, len(cache.events))
}

func TestPersistenceFailure(t *testing.T) {
	mockDB := NewMockDB()
	mockDB.setPersistenceFailure(true)
	cache := NewEventCache()
	client := &MockClient{}
	events := []*si.EventRecord{
		{TimestampNano: 100, Type: si.EventRecord_APP, ObjectID: "app-1"},
		{TimestampNano: 200, Type: si.EventRecord_APP, ObjectID: "app-1"},
	}
	client.response = &dao.EventRecordDAO{
		LowestID:     0,
		HighestID:    1,
		InstanceUUID: "yunikornUUID",
		EventRecords: events,
	}
	reader := NewEventWriter(mockDB, client, cache)

	err := reader.fetchAndPersistEvents(context.Background())
	assert.ErrorContains(t, err, "error while storing events")
	assert.Equal(t, 1, len(mockDB.getPersistenceCalls()))
	assert.Equal(t, 0, len(cache.events))
}

func TestGetValidStartID(t *testing.T) {
	client := &MockClient{}
	client.response = &dao.EventRecordDAO{
		LowestID:     12345,
		HighestID:    222222,
		InstanceUUID: "yunikornUUID",
	}
	reader := NewEventWriter(NewMockDB(), client, NewEventCache())

	reader.getValidStartID(context.Background())

	assert.Equal(t, uint64(12345), reader.startID)
}

func TestGetValidStartIDWithFailure(t *testing.T) {
	client := &MockClient{}
	client.response = &dao.EventRecordDAO{
		LowestID:     12345,
		HighestID:    222222,
		InstanceUUID: "yunikornUUID",
	}
	client.setFailure(true)
	reader := NewEventWriter(NewMockDB(), client, NewEventCache())

	go func() {
		time.Sleep(2 * time.Second)
		client.setFailure(false)
	}()

	reader.getValidStartID(context.Background())
	assert.Equal(t, uint64(12345), reader.startID)
}
