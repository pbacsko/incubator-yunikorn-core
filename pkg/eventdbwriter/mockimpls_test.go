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

	persistFails   bool
	getEventsFails bool
	removeFails    bool

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

	if ms.persistFails {
		return fmt.Errorf("error while storing events")
	}

	return nil
}

func (ms *MockDB) GetAllEventsForApp(_ context.Context, appID string) ([]*si.EventRecord, error) {
	ms.Lock()
	defer ms.Unlock()

	if ms.getEventsFails {
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

func (ms *MockDB) RemoveOldEntries(_ context.Context, cutoff time.Time) (int64, error) {
	ms.Lock()
	defer ms.Unlock()

	ms.removeCalls = append(ms.removeCalls, RemoveCall{
		cutoff: cutoff,
	})

	if ms.removeFails {
		return 0, fmt.Errorf("error while removing records")
	}

	return ms.numRemoved, nil
}

func (ms *MockDB) setEvents(events []*si.EventRecord) {
	ms.Lock()
	defer ms.Unlock()
	ms.events = events
}

func (ms *MockDB) setPersistenceFailure(b bool) {
	ms.Lock()
	defer ms.Unlock()
	ms.persistFails = b
}

func (ms *MockDB) setRemoveFailure(b bool) {
	ms.Lock()
	defer ms.Unlock()
	ms.removeFails = b
}

func (ms *MockDB) setDbFetchFailure(b bool) {
	ms.Lock()
	defer ms.Unlock()
	ms.getEventsFails = b
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
	failure  bool
	response *dao.EventRecordDAO
	sync.Mutex
}

func (mc *MockClient) GetRecentEvents(_ uint64) (*dao.EventRecordDAO, error) {
	mc.Lock()
	defer mc.Unlock()

	if mc.failure {
		return nil, fmt.Errorf("error while getting events")
	}

	return mc.response, nil
}

func (mc *MockClient) setFailure(b bool) {
	mc.Lock()
	defer mc.Unlock()
	mc.failure = b
}
