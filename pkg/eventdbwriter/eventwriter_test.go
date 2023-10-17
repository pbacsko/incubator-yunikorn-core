package eventdbwriter

import (
	"context"
	"math"
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
	writer := NewEventWriter(mockDB, client, NewEventCache())

	err := writer.fetchAndPersistEvents(context.Background())
	assert.NilError(t, err)
	assert.Equal(t, "yunikornUUID", writer.ykID)

	assert.Equal(t, 1, len(mockDB.getPersistenceCalls()))
	assert.Equal(t, "yunikornUUID", mockDB.getYunikornID())
	call := mockDB.getPersistenceCalls()[0]
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
	writer := NewEventWriter(mockDB, client, NewEventCache())

	// first round, two events
	err := writer.fetchAndPersistEvents(context.Background())
	assert.NilError(t, err)
	assert.Equal(t, "yunikornUUID", writer.ykID)

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
	err = writer.fetchAndPersistEvents(context.Background())
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
	assert.Equal(t, uint64(4), writer.eventID.Load())

	// third round, no new events
	err = writer.fetchAndPersistEvents(context.Background())
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

	err = writer.fetchAndPersistEvents(context.Background())
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
	writer := NewEventWriter(mockDB, client, cache)

	err := writer.fetchAndPersistEvents(context.Background())
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
	err = writer.fetchAndPersistEvents(context.Background())
	assert.NilError(t, err)
	assert.Equal(t, 0, len(cache.events))
	assert.Assert(t, !writer.haveEventID)

	// second cycle after restart
	err = writer.fetchAndPersistEvents(context.Background())
	assert.NilError(t, err)
	assert.Equal(t, "yunikornUUID-2", writer.ykID)
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
	writer := NewEventWriter(mockDB, client, cache)
	client.setFailure(true)

	err := writer.fetchAndPersistEvents(context.Background())
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
	writer := NewEventWriter(mockDB, client, cache)

	err := writer.fetchAndPersistEvents(context.Background())
	assert.ErrorContains(t, err, "error while storing events")
	assert.Equal(t, 1, len(mockDB.getPersistenceCalls()))
	assert.Equal(t, 0, len(cache.events))
}

func TestStartIDChangesAfterInitialFetch(t *testing.T) {
	// test special case: we fetch the lowest id, then it becomes invalid immediately, because
	// the ring buffer is full and the oldest event is overwritten.
	client := &MockClient{}

	// use callback function, so we can react to the tight for loop quickly
	client.setOnGetEvents(func(start uint64) {
		// first call to get start ID
		if start == math.MaxUint64 {
			client.setContentsNoLock("yunikornUUID", nil, 100, 102)
			return
		}

		// second call with start == 100, return a new "lowestID", ie. the previous is no longer valid
		if start == 100 {
			client.setContentsNoLock("yunikornUUID", nil, 103, 105)
			return
		}

		// third call with start == 103, perform yet another round
		if start == 103 {
			client.setContentsNoLock("yunikornUUID", nil, 105, 107)
			return
		}

		// final round
		if start == 105 {
			client.setContentsNoLock("yunikornUUID", []*si.EventRecord{
				{TimestampNano: 100, Type: si.EventRecord_APP, ObjectID: "app-1"},
				{TimestampNano: 200, Type: si.EventRecord_APP, ObjectID: "app-1"},
				{TimestampNano: 300, Type: si.EventRecord_APP, ObjectID: "app-1"},
			}, 105, 107)
			client.setOnGetEventsNoLock(nil) // disable this callback
			return
		}

		t.Errorf("Unexpected argument for callback function: %d", start)
	})

	mockDB := NewMockDB()
	writer := NewEventWriter(mockDB, client, NewEventCache())

	err := writer.fetchAndPersistEvents(context.Background())
	assert.NilError(t, err)
	persistence := mockDB.getPersistenceCalls()
	assert.Equal(t, 1, len(persistence))
	call := persistence[0]
	assert.Equal(t, 3, len(call.events))
	assert.Equal(t, "yunikornUUID", call.yunikornID)
	assert.Equal(t, uint64(105), call.startEventID)
	assert.Equal(t, int64(100), call.events[0].TimestampNano)
	assert.Equal(t, int64(200), call.events[1].TimestampNano)
	assert.Equal(t, int64(300), call.events[2].TimestampNano)
}

func TestEventPersistenceBackground(t *testing.T) {
	mockDB := NewMockDB()
	client := &MockClient{}
	events := []*si.EventRecord{
		{TimestampNano: 100, ObjectID: "app-1"},
		{TimestampNano: 200, ObjectID: "app-1"},
	}
	client.setContents("yunikornUUID", events, 3, 4)
	writer := NewEventWriter(mockDB, client, NewEventCache())
	writer.eventFetchPeriod = 10 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	writer.Start(ctx)

	// first round: persistence of two events
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, uint64(5), writer.eventID.Load())
	persistence := mockDB.getPersistenceCalls()
	assert.Equal(t, 1, len(persistence))
	call := persistence[0]
	assert.Equal(t, "yunikornUUID", call.yunikornID)
	assert.Equal(t, uint64(3), call.startEventID)
	assert.Equal(t, 2, len(call.events))
	assert.Equal(t, int64(100), call.events[0].TimestampNano)
	assert.Equal(t, int64(200), call.events[1].TimestampNano)

	// second round: add two new events
	events = append(events, &si.EventRecord{
		TimestampNano: 300,
		ObjectID:      "app-1"})
	events = append(events, &si.EventRecord{
		TimestampNano: 400,
		ObjectID:      "app-1"})
	client.setContents("yunikornUUID", events, 3, 6)
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, uint64(7), writer.eventID.Load())
	persistence = mockDB.getPersistenceCalls()
	assert.Equal(t, 2, len(persistence))
	call = persistence[1]
	assert.Equal(t, "yunikornUUID", call.yunikornID)
	assert.Equal(t, uint64(5), call.startEventID)
	assert.Equal(t, 2, len(call.events))
	assert.Equal(t, int64(300), call.events[0].TimestampNano)
	assert.Equal(t, int64(400), call.events[1].TimestampNano)

	// third round: simulating YK restart
	events = []*si.EventRecord{
		{TimestampNano: 1000, ObjectID: "app-1"},
		{TimestampNano: 1100, ObjectID: "app-1"},
	}
	client.setContents("yunikornUUID-2", events, 0, 1)
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, uint64(2), writer.eventID.Load())
	persistence = mockDB.getPersistenceCalls()
	assert.Equal(t, 3, len(persistence))
	call = persistence[2]
	assert.Equal(t, "yunikornUUID-2", call.yunikornID)
	assert.Equal(t, uint64(0), call.startEventID)
	assert.Equal(t, 2, len(call.events))
	assert.Equal(t, int64(1000), call.events[0].TimestampNano)
	assert.Equal(t, int64(1100), call.events[1].TimestampNano)

	// check cancellation
	cancel()
	events = append(events, &si.EventRecord{
		TimestampNano: 1200,
		ObjectID:      "app-1"})
	client.setContents("yunikornUUID-2", events, 0, 2)
	time.Sleep(100 * time.Millisecond)
	persistence = mockDB.getPersistenceCalls()
	assert.Equal(t, 3, len(persistence))
	assert.Equal(t, uint64(2), writer.eventID.Load())
}
