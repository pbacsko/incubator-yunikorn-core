package eventdbwriter

import (
	"log"
	"sync"
	"time"

	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

var expiry = 15 * time.Minute

// EventCache stores application events to speed up REST queries.
// Completed applications are removed after 15 minutes.
type EventCache struct {
	events         map[string][]*si.EventRecord // events per app
	fullHistory    map[string]bool              // whether we have a full history for an app
	completionTime map[string]time.Time         // completion time per app

	sync.Mutex
}

func NewEventCache() *EventCache {
	return &EventCache{
		events:         make(map[string][]*si.EventRecord),
		fullHistory:    make(map[string]bool),
		completionTime: make(map[string]time.Time),
	}
}

func (c *EventCache) SetHaveFullHistory(appID string) {
	c.Lock()
	defer c.Unlock()
	c.fullHistory[appID] = true
}

func (c *EventCache) AddEvent(appID string, event *si.EventRecord) {
	c.Lock()
	defer c.Unlock()
	c.addEvent(appID, event)
}

func (c *EventCache) addEvent(appID string, event *si.EventRecord) {
	c.events[appID] = append(c.events[appID], event)
	if event.EventChangeDetail == si.EventRecord_APP_COMPLETED {
		log.Printf("Application %s completed", appID)
		c.completionTime[appID] = time.Now()
	}
}

func (c *EventCache) AddEvents(appID string, events []*si.EventRecord) {
	c.Lock()
	defer c.Unlock()
	for _, event := range events {
		c.addEvent(appID, event)
	}
}

func (c *EventCache) GetEvents(appID string) []*si.EventRecord {
	c.Lock()
	defer c.Unlock()

	if !c.fullHistory[appID] {
		return nil
	}
	return c.events[appID]
}

func (c *EventCache) Start() {
	go func() {
		for {
			removed := c.cleanUpOldEntries()
			if removed > 0 {
				log.Printf("Event cache: removed %d expired entries", removed)
			}
			time.Sleep(30 * time.Second)
		}
	}()
}

func (c *EventCache) Clear() {
	c.Lock()
	defer c.Unlock()
	log.Println("Clearing event cache")
	c.events = make(map[string][]*si.EventRecord)
	c.fullHistory = make(map[string]bool)
	c.completionTime = make(map[string]time.Time)
}

func (c *EventCache) cleanUpOldEntries() int {
	c.Lock()
	defer c.Unlock()

	removed := 0
	for appID, completed := range c.completionTime {
		if time.Since(completed) > expiry {
			log.Printf("Removing application %s from the event cache",
				appID)
			delete(c.events, appID)
			delete(c.completionTime, appID)
			delete(c.fullHistory, appID)
			removed++
		}
	}

	return removed
}
