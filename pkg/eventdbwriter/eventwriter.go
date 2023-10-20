package eventdbwriter

import (
	"context"
	"math"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/apache/yunikorn-core/pkg/webservice/dao"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

const defaultFetchPeriod = 2 * time.Second

// EventWriter periodically retrieves events from Yunikorn and persists them by using
// the underlying storage object.
type EventWriter struct {
	storage     Storage
	client      YunikornClient
	cache       *EventCache
	ykID        string        // yunikorn instance ID
	eventID     atomic.Uint64 // current lowest event id for event retrieval, passed to the client
	haveEventID bool          // whether we have id set or not

	eventFetchPeriod time.Duration
}

func NewEventWriter(storage Storage, client YunikornClient, cache *EventCache) *EventWriter {
	return &EventWriter{
		storage:          storage,
		client:           client,
		cache:            cache,
		eventFetchPeriod: defaultFetchPeriod,
		haveEventID:      false,
	}
}

func (e *EventWriter) Start(ctx context.Context) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(e.eventFetchPeriod):
				err := e.fetchAndPersistEvents(ctx)
				if err != nil {
					GetLogger().Error("Unable to read/save events from Yunikorn", zap.Error(err))
				}
			}
		}
	}()
}

func (e *EventWriter) fetchAndPersistEvents(ctx context.Context) error {
	result, eventID, err := e.fetchEvents(ctx)
	if err != nil {
		if result != nil && result.ykError != nil {
			GetLogger().Error("Received error message from Yunikorn",
				zap.String("message", result.ykError.Message),
				zap.String("description", result.ykError.Description),
				zap.Int("status code", result.ykError.StatusCode))
			if result.ykError.Message == "Event tracking is disabled" {
				GetLogger().Error("Event tracking is DISABLED inside Yunikorn. No events are persisted until this is changed.")
			}
		}
		return err
	}

	events := result.eventRecord
	uuid := events.InstanceUUID
	e.storage.SetYunikornID(uuid)
	if e.checkRestart(uuid) {
		return nil
	}
	e.ykID = uuid

	if len(events.EventRecords) == 0 {
		return nil
	}
	err = e.persistEvents(ctx, eventID, events)
	if err != nil {
		return err
	}

	// update startID - next batch will start at this number in the next iteration
	e.eventID.Store(events.HighestID + 1)

	return nil
}

func (e *EventWriter) fetchEvents(ctx context.Context) (*EventQueryResult, uint64, error) {
	eventID := e.eventID.Load()

	// Need a tight loop here: if the ring buffer is written quickly,
	// then the lowest valid ID is also changing quickly.
	// So we update it from the empty response and fetch all events again.
	// This can happen during startup.
	for {
		if !e.haveEventID {
			// make sure we don't retrieve any valid record by accident
			eventID = math.MaxUint64
		}
		result, err := e.client.GetRecentEvents(ctx, eventID)
		if err != nil {
			return result, 0, err
		}
		events := result.eventRecord

		if !e.haveEventID || (len(events.EventRecords) == 0 && events.LowestID > eventID) {
			if e.haveEventID {
				GetLogger().Info("Adjusting event ID, current one became invalid", zap.Uint64("current", eventID),
					zap.Uint64("new", events.LowestID))
			} else {
				GetLogger().Info("Setting valid event ID",
					zap.Uint64("new", events.LowestID))
			}

			e.eventID.Store(events.LowestID)
			e.haveEventID = true
			eventID = events.LowestID
			continue
		}

		return result, eventID, nil
	}
}

func (e *EventWriter) checkRestart(uuid string) bool {
	// No matter if we have new events or not, we need to detect the lowest id again,
	// so just do some bookkeeping and collect events in the next cycle.
	if e.ykID != "" && e.ykID != uuid {
		GetLogger().Info("Yunikorn restart detected",
			zap.String("last uuid", e.ykID), zap.String("new uuid", uuid))
		e.cache.Clear()
		e.ykID = uuid
		e.haveEventID = false
		return true
	}

	return false
}

func (e *EventWriter) persistEvents(ctx context.Context, eventID uint64, events *dao.EventRecordDAO) error {
	err := e.storage.PersistEvents(ctx, eventID, events.EventRecords)
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

	return nil
}
