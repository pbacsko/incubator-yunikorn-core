package eventdbwriter

import (
	"context"
	"math"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

const defaultFetchPeriod = 2 * time.Second
const defaultStartIDFetchPeriod = time.Second

var eventFetchPeriod = defaultFetchPeriod
var startIdFetchPeriod = defaultStartIDFetchPeriod

// EventWriter periodically retrieves events from Yunikorn and persists them by using
// the underlying storage object.
type EventWriter struct {
	storage Storage
	client  YunikornClient
	cache   *EventCache
	ykID    string
	startID atomic.Uint64
}

func NewEventWriter(storage Storage, client YunikornClient, cache *EventCache) *EventWriter {
	return &EventWriter{
		storage: storage,
		client:  client,
		cache:   cache,
	}
}

func (e *EventWriter) getValidStartID(ctx context.Context) {
	err := e.tryGetValidStartIDOnce()
	if err == nil {
		return
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(startIdFetchPeriod):
			err := e.tryGetValidStartIDOnce()
			if err == nil {
				return
			}
		}
	}
}

func (e *EventWriter) tryGetValidStartIDOnce() error {
	eventDao, err := e.client.GetRecentEvents(math.MaxUint64)
	if err != nil {
		GetLogger().Error("Coult not fetch events", zap.Error(err))
		return err
	}
	GetLogger().Info("Lowest valid event ID", zap.Uint64("id", eventDao.LowestID))
	e.startID.Store(eventDao.LowestID)
	return nil
}

func (e *EventWriter) Start(ctx context.Context) {
	go func(period time.Duration) {
		e.getValidStartID(ctx)
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(period):
				err := e.fetchAndPersistEvents(ctx)
				if err != nil {
					GetLogger().Error("Unable to fetch events from Yunikorn", zap.Error(err))
				}
			}
		}
	}(eventFetchPeriod)
}

func (e *EventWriter) fetchAndPersistEvents(ctx context.Context) error {
	startID := e.startID.Load()
	events, err := e.client.GetRecentEvents(startID)
	if err != nil {
		return err
	}

	// corner case which can happen during startup
	if len(events.EventRecords) == 0 && events.LowestID > startID {
		GetLogger().Info("Adjusting startID", zap.Uint64("current", startID),
			zap.Uint64("new", events.LowestID))
		e.startID.Store(events.LowestID)
		return nil
	}

	uuid := events.InstanceUUID
	e.storage.SetYunikornID(uuid)
	// Restart detection
	//
	// No matter if we have new events or not, we need to detect the lowest id again,
	// so just do some bookkeeping and collect events in the next cycle.
	if e.ykID != "" && e.ykID != uuid {
		GetLogger().Info("Yunikorn restart detected",
			zap.String("last uuid", e.ykID), zap.String("new uuid", uuid))
		e.cache.Clear()
		e.ykID = uuid
		e.getValidStartID(ctx)
		return nil
	}
	e.ykID = uuid

	if len(events.EventRecords) == 0 {
		return nil
	}

	err = e.storage.PersistEvents(ctx, startID, events.EventRecords)
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
	e.startID.Store(events.HighestID + 1)

	return nil
}
