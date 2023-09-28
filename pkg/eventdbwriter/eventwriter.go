package eventdbwriter

import (
	"math"
	"time"

	"go.uber.org/zap"

	"github.com/apache/yunikorn-core/pkg/webservice/dao"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

var eventFetchPeriod = 2 * time.Second

// EventWriter periodically retrieves events from Yunikorn and persists them by using
// the underlying storage object.
type EventWriter struct {
	storage Storage
	client  YunikornClient
	cache   *EventCache
	ykID    string
	startID uint64
}

func NewEventWriter(storage Storage, client YunikornClient, cache *EventCache) *EventWriter {
	return &EventWriter{
		storage: storage,
		client:  client,
		cache:   cache,
	}
}

func (e *EventWriter) getValidStartID() {
	var eventDao *dao.EventRecordDAO
	for {
		var err error
		eventDao, err = e.client.GetRecentEvents(math.MaxUint64)
		if err != nil {
			GetLogger().Error("Coult not fetch events, retrying", zap.Error(err))
			time.Sleep(time.Second)
			continue
		}
		break
	}
	GetLogger().Info("Lowest valid event ID", zap.Uint64("id", eventDao.LowestID))
	e.startID = eventDao.LowestID
}

func (e *EventWriter) Start() {
	go func() {
		e.getValidStartID()
		for {
			time.Sleep(eventFetchPeriod)
			err := e.fetchAndPersistEvents()
			if err != nil {
				GetLogger().Error("Unable to fetch events from Yunikorn", zap.Error(err))
			}
		}
	}()
}

func (e *EventWriter) fetchAndPersistEvents() error {
	events, err := e.client.GetRecentEvents(e.startID)
	if err != nil {
		return err
	}

	uuid := events.InstanceUUID
	if e.ykID != "" && e.ykID != uuid {
		GetLogger().Info("Yunikorn restart detected",
			zap.String("last uuid", e.ykID), zap.String("new uuid", uuid))
		e.cache.Clear()
		e.startID = events.LowestID // expected to be "0"
	}
	e.storage.SetYunikornID(uuid)
	e.ykID = uuid

	if len(events.EventRecords) == 0 {
		return nil
	}

	err = e.storage.PersistEvents(e.startID, events.EventRecords)
	if err != nil {
		GetLogger().Error("Failed to persist events", zap.Error(err))
		return err
	}
	for _, event := range events.EventRecords {
		if event.Type == si.EventRecord_APP {
			e.cache.AddEvent(event.ObjectID, event)
			// we have the first event from an app, so it's safe to read the history back from the cache w/o DB access
			if event.EventChangeType == si.EventRecord_ADD && event.EventChangeDetail == si.EventRecord_DETAILS_NONE {
				e.cache.SetHaveFullHistory(event.ObjectID)
			}
		}
	}
	GetLogger().Info("Persisted new events",
		zap.Int("number of events", len(events.EventRecords)),
		zap.Uint64("lowest id", events.LowestID),
		zap.Uint64("highest id", events.HighestID))

	// update startID - next batch will start at this number in the next iteration
	e.startID = events.HighestID + 1

	return nil
}
