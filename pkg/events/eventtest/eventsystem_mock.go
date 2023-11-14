package eventtest

import (
	"sync"

	"github.com/apache/yunikorn-core/pkg/events"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

// EventSystemMock simple event mock that is used exclusively for testing from various packages.
type EventSystemMock struct {
	events        []*si.EventRecord
	enabled       bool
	low           uint64
	high          uint64
	stream        chan *si.EventRecord
	activeStreams int

	// record stuff from CreateEventStream()
	createCount   uint64
	streamCreated bool

	sync.Mutex
}

func (m *EventSystemMock) CreateEventStream(_ string, count uint64) *events.EventStream {
	m.Lock()
	defer m.Unlock()
	m.activeStreams++
	m.createCount = count
	m.streamCreated = true
	return &events.EventStream{
		Events: m.stream,
	}
}

func (m *EventSystemMock) RemoveStream(_ *events.EventStream) {
	m.Lock()
	defer m.Unlock()
	m.activeStreams--
}

func (m *EventSystemMock) AddEvent(event *si.EventRecord) {
	m.Lock()
	defer m.Unlock()
	m.events = append(m.events, event)
}

func (m *EventSystemMock) StartService() {}

func (m *EventSystemMock) Stop() {}

func (m *EventSystemMock) Reset() {
	m.Lock()
	defer m.Unlock()
	m.events = make([]*si.EventRecord, 0)
}

func (m *EventSystemMock) GetEventsFromID(start, count uint64) ([]*si.EventRecord, uint64, uint64) {
	if start > m.high || start < m.low {
		return nil, m.low, m.high
	}

	var filtered []*si.EventRecord
	idx := 0
	for i := m.low; i <= m.high; i++ {
		if i >= start {
			filtered = append(filtered, m.events[idx])
		}
		if uint64(len(filtered)) == count {
			break
		}
		idx++
	}

	return filtered, m.low, m.high
}

func (m *EventSystemMock) IsEventTrackingEnabled() bool {
	m.Lock()
	defer m.Unlock()
	return m.enabled
}

func (m *EventSystemMock) SetStreamChannel(ch chan *si.EventRecord) {
	m.Lock()
	defer m.Unlock()
	m.stream = ch
}

func (m *EventSystemMock) GetActiveStreams() int {
	m.Lock()
	defer m.Unlock()
	return m.activeStreams
}

func (m *EventSystemMock) SetIDRange(low, high uint64) {
	m.Lock()
	defer m.Unlock()
	m.low = low
	m.high = high
}

func (m *EventSystemMock) GetEvents() []*si.EventRecord {
	m.Lock()
	defer m.Unlock()
	return m.events
}

func (m *EventSystemMock) GetCountForCreateStream() uint64 {
	m.Lock()
	defer m.Unlock()
	return m.createCount
}

func (m *EventSystemMock) HasCreatedStream() bool {
	m.Lock()
	defer m.Unlock()
	return m.streamCreated
}

func (m *EventSystemMock) ResetStreamCalls() {
	m.Lock()
	defer m.Unlock()
	m.createCount = 0
	m.streamCreated = false
}

func NewEventSystemMock(enabled bool) *EventSystemMock {
	return &EventSystemMock{events: make([]*si.EventRecord, 0), enabled: enabled}
}
