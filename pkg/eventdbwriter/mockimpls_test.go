package eventdbwriter

import (
	"context"
	"fmt"
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

type MockDB struct {
	events           []*si.EventRecord
	persistenceCalls []PersistenceCall
	removeCalls      []RemoveCall
	numRemoved       int64
	ykID             string

	dbFailure bool

	sync.Mutex
}

func (ms *MockDB) SetYunikornID(yunikornID string) {
	ms.Lock()
	defer ms.Unlock()

	ms.ykID = yunikornID
}

func NewMockDB() *MockDB {
	return &MockDB{
		events: make([]*si.EventRecord, 0),
	}
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
		return fmt.Errorf("error while storing events")
	}

	return nil
}

func (ms *MockDB) GetAllEventsForApp(_ context.Context, appID string) ([]*si.EventRecord, error) {
	ms.Lock()
	defer ms.Unlock()

	if ms.dbFailure {
		return nil, fmt.Errorf("error while fetching events")
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
		return 0, fmt.Errorf("error while removing records")
	}

	return ms.numRemoved, nil
}

func (ms *MockDB) setEvents(events []*si.EventRecord) {
	ms.Lock()
	defer ms.Unlock()
	ms.events = events
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
		return nil, fmt.Errorf("error while getting events")
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