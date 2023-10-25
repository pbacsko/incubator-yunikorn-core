package eventdbwriter

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/apache/yunikorn-core/pkg/webservice/dao"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

type PersistenceCall struct {
	yunikornID   string
	startEventID uint64
	events       []*si.EventRecord
}

type RemoveCall struct {
	cutoff time.Time
}

type LastEventCall struct {
	ykID string
}

type MockDB struct {
	events           []*si.EventRecord
	persistenceCalls []PersistenceCall
	removeCalls      []RemoveCall
	lastEventCalls   []LastEventCall
	numRemoved       int64
	ykID             string
	lastEvent        *si.EventRecord
	lastID           uint64

	dbFailure bool

	sync.Mutex
}

func NewMockDB() *MockDB {
	return &MockDB{
		events: make([]*si.EventRecord, 0),
	}
}

func (ms *MockDB) SetYunikornID(yunikornID string) {
	ms.Lock()
	defer ms.Unlock()

	ms.ykID = yunikornID
}

func (ms *MockDB) PersistEvents(_ context.Context, startEventID uint64, events []*si.EventRecord) error {
	ms.Lock()
	defer ms.Unlock()

	ms.persistenceCalls = append(ms.persistenceCalls, PersistenceCall{
		yunikornID:   ms.ykID,
		startEventID: startEventID,
		events:       events,
	})

	if ms.dbFailure {
		return errors.New("error while storing events")
	}

	return nil
}

func (ms *MockDB) GetAllEventsForApp(_ context.Context, appID string) ([]*si.EventRecord, error) {
	ms.Lock()
	defer ms.Unlock()

	if ms.dbFailure {
		return nil, errors.New("error while fetching events")
	}

	var result []*si.EventRecord
	for _, event := range ms.events {
		if event.ObjectID == appID {
			result = append(result, event)
		}
	}

	return result, nil
}

func (ms *MockDB) RemoveObsoleteEntries(_ context.Context, cutoff time.Time) (int64, error) {
	ms.Lock()
	defer ms.Unlock()

	ms.removeCalls = append(ms.removeCalls, RemoveCall{
		cutoff: cutoff,
	})

	if ms.dbFailure {
		return 0, errors.New("error while removing records")
	}

	return ms.numRemoved, nil
}

func (ms *MockDB) GetLastEvent(_ context.Context, ykID string) (uint64, *si.EventRecord, error) {
	ms.Lock()
	defer ms.Unlock()

	ms.lastEventCalls = append(ms.lastEventCalls, LastEventCall{
		ykID: ykID,
	})
	if ms.dbFailure {
		return 0, nil, errors.New("error while fetching events")
	}

	return ms.lastID, ms.lastEvent, nil
}

func (ms *MockDB) setEvents(events []*si.EventRecord) {
	ms.Lock()
	defer ms.Unlock()
	ms.events = events
}

func (ms *MockDB) setLastEventData(lastID uint64, event *si.EventRecord) {
	ms.Lock()
	defer ms.Unlock()
	ms.lastID = lastID
	ms.lastEvent = event
}

func (ms *MockDB) setDBFailure(b bool) {
	ms.Lock()
	defer ms.Unlock()
	ms.dbFailure = b
}

func (ms *MockDB) setNumRemoved(n int64) {
	ms.Lock()
	defer ms.Unlock()
	ms.numRemoved = n
}

func (ms *MockDB) getRemoveCalls() []RemoveCall {
	ms.Lock()
	defer ms.Unlock()
	return ms.removeCalls
}

func (ms *MockDB) getPersistenceCalls() []PersistenceCall {
	ms.Lock()
	defer ms.Unlock()
	return ms.persistenceCalls
}

func (ms *MockDB) getLastEventCalls() []LastEventCall {
	ms.Lock()
	defer ms.Unlock()
	return ms.lastEventCalls
}

func (ms *MockDB) getYunikornID() string {
	ms.Lock()
	defer ms.Unlock()
	return ms.ykID
}

type MockClient struct {
	failure     bool
	events      []*si.EventRecord
	low         uint64
	high        uint64
	ykID        string
	onGetEvents func(uint64)

	sync.Mutex
}

func (mc *MockClient) GetRecentEvents(_ context.Context, start uint64) (*EventQueryResult, error) {
	mc.Lock()
	defer mc.Unlock()

	if mc.onGetEvents != nil {
		mc.onGetEvents(start)
	}

	if mc.failure {
		return nil, errors.New("error while getting events")
	}

	var filtered []*si.EventRecord
	id := mc.low
	var responseLow uint64
	for _, e := range mc.events {
		if id >= start {
			filtered = append(filtered, e)
			if len(filtered) == 1 {
				responseLow = id
			}
		}
		id++
	}

	if len(filtered) == 0 {
		return &EventQueryResult{
			eventRecord: &dao.EventRecordDAO{
				InstanceUUID: mc.ykID,
				LowestID:     mc.low,
				HighestID:    mc.high,
			},
		}, nil
	}

	lowest := responseLow
	highest := id - 1

	return &EventQueryResult{
		eventRecord: &dao.EventRecordDAO{
			InstanceUUID: mc.ykID,
			LowestID:     lowest,
			HighestID:    highest,
			EventRecords: filtered,
		},
	}, nil
}

func (mc *MockClient) setFailure(b bool) {
	mc.Lock()
	defer mc.Unlock()
	mc.failure = b
}

func (mc *MockClient) setContents(ykID string, events []*si.EventRecord, low, high uint64) {
	mc.Lock()
	defer mc.Unlock()
	mc.events = events
	mc.ykID = ykID
	mc.low = low
	mc.high = high
}

// invoked from callback onGetEvents
func (mc *MockClient) setContentsNoLock(ykID string, events []*si.EventRecord, low, high uint64) {
	mc.events = events
	mc.ykID = ykID
	mc.low = low
	mc.high = high
}

func (mc *MockClient) setOnGetEvents(f func(uint64)) {
	mc.Lock()
	defer mc.Unlock()
	mc.onGetEvents = f
}

// invoked from callback onGetEvents
func (mc *MockClient) setOnGetEventsNoLock(f func(uint64)) {
	mc.onGetEvents = f
}
