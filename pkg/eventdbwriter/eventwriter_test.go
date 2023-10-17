package eventdbwriter

import (
	"context"
	"testing"
	"time"

	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

func TestSimpleEventPersistence(t *testing.T) {
	mockDB := NewMockDB()
	client := &MockClient{}
	client.setContents("yunikornUUID", []*si.EventRecord{
		{TimestampNano: 100, ObjectID: "app-1"},
		{TimestampNano: 200, ObjectID: "app-1"},
	}, 0, 1)
	reader := NewEventWriter(mockDB, client, NewEventCache())
	reader.startID = 0

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
	client.setContents("yunikornUUID", events, 0, 1)
	reader := NewEventWriter(mockDB, client, NewEventCache())

	// first round, two events
	err := reader.fetchAndPersistEvents(context.Background())
	assert.NilError(t, err)
	assert.Equal(t, "yunikornUUID", reader.ykID)

	// second round, two new events (4 in total)
	events = append(events, &si.EventRecord{
		TimestampNano:     300,
		Type:              si.EventRecord_APP,
		EventChangeType:   si.EventRecord_SET,
		EventChangeDetail: si.EventRecord_APP_RUNNING,
		ObjectID:          "app-1"})
	events = append(events, &si.EventRecord{
		TimestampNano:     400,
		Type:              si.EventRecord_NODE,
		EventChangeType:   si.EventRecord_ADD,
		EventChangeDetail: si.EventRecord_NODE_ALLOC,
		ObjectID:          "node-1"})
	client.setContents("yunikornUUID", events, 0, 3)
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
	err = reader.fetchAndPersistEvents(context.Background())
	assert.NilError(t, err)
	assert.Equal(t, 2, len(mockDB.getPersistenceCalls()))

	// fourth round, one new event
	events = append(events, &si.EventRecord{
		TimestampNano:     500,
		Type:              si.EventRecord_APP,
		EventChangeType:   si.EventRecord_SET,
		EventChangeDetail: si.EventRecord_APP_COMPLETING,
		ObjectID:          "app-1"})
	client.setContents("yunikornUUID", events, 0, 4)

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
	client.setContents("yunikornUUID", events, 0, 1)
	reader := NewEventWriter(mockDB, client, cache)

	err := reader.fetchAndPersistEvents(context.Background())
	assert.NilError(t, err)

	// next response with a different InstanceUUID
	events = []*si.EventRecord{
		{TimestampNano: 11, Type: si.EventRecord_APP,
			EventChangeType:   si.EventRecord_ADD,
			EventChangeDetail: si.EventRecord_DETAILS_NONE,
			ObjectID:          "app-1"},
		{TimestampNano: 22, Type: si.EventRecord_APP, ObjectID: "app-1"},
	}
	client.setContents("yunikornUUID-2", events, 111, 112)

	// first cycle after restart, we detect the restart (no new events here)
	err = reader.fetchAndPersistEvents(context.Background())
	assert.NilError(t, err)
	assert.Equal(t, 0, len(cache.events))
	assert.Equal(t, uint64(111), reader.startID)

	// second cycle after restart
	err = reader.fetchAndPersistEvents(context.Background())
	assert.NilError(t, err)
	assert.Equal(t, "yunikornUUID-2", reader.ykID)
	assert.Equal(t, "yunikornUUID-2", mockDB.getYunikornID())
	call := mockDB.getPersistenceCalls()[1]
	assert.Equal(t, 2, len(call.events))
	assert.Equal(t, "yunikornUUID-2", call.yunikornID)
	assert.Equal(t, uint64(111), call.startEventID)
	assert.Equal(t, 2, len(call.events))
	assert.Equal(t, int64(11), call.events[0].TimestampNano)
	assert.Equal(t, int64(22), call.events[1].TimestampNano)
}

func TestClientFailure(t *testing.T) {
	mockDB := NewMockDB()
	cache := NewEventCache()
	client := &MockClient{}
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
	client.setContents("yunikornUUID", events, 0, 1)
	reader := NewEventWriter(mockDB, client, cache)

	err := reader.fetchAndPersistEvents(context.Background())
	assert.ErrorContains(t, err, "error while storing events")
	assert.Equal(t, 1, len(mockDB.getPersistenceCalls()))
	assert.Equal(t, 0, len(cache.events))
}

func TestGetValidStartID(t *testing.T) {
	client := &MockClient{}
	client.setContents("yunikornUUID", nil, 12345, 222222)
	reader := NewEventWriter(NewMockDB(), client, NewEventCache())

	reader.getValidStartID(context.Background())

	assert.Equal(t, uint64(12345), reader.startID)
}

func TestGetValidStartIDWithFailure(t *testing.T) {
	client := &MockClient{}
	client.setContents("yunikornUUID", nil, 12345, 222222)
	client.setFailure(true)
	reader := NewEventWriter(NewMockDB(), client, NewEventCache())

	go func() {
		time.Sleep(2 * time.Second)
		client.setFailure(false)
	}()

	reader.getValidStartID(context.Background())
	assert.Equal(t, uint64(12345), reader.startID)
}
