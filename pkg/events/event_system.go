/*
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/

package events

import (
	"fmt"
	"sync"
	"time"

	"github.com/apache/yunikorn-core/pkg/common"
	"github.com/apache/yunikorn-core/pkg/common/configs"
	"github.com/apache/yunikorn-core/pkg/log"
	"github.com/apache/yunikorn-core/pkg/metrics"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

// need to change for testing
var defaultEventChannelSize = 100000
var defaultRingBufferSize uint64 = 100000

var once sync.Once
var ev EventSystem

type EventSystem interface {
	AddEvent(event *si.EventRecord)
	StartService()
	Stop()
	IsEventTrackingEnabled() bool
	GetEventsFromID(uint64, uint64) ([]*si.EventRecord, uint64, uint64)
	CreateEventStream(string, uint64) *EventStream
	RemoveStream(*EventStream)
}

type EventSystemImpl struct {
	eventSystemId string
	Store         *EventStore // storing eventChannel
	publisher     *EventPublisher
	eventBuffer   *eventRingBuffer
	streaming     *EventStreaming

	channel chan *si.EventRecord // channelling input eventChannel
	stop    chan bool            // whether the service is stopped
	stopped bool

	trackingEnabled    bool
	requestCapacity    int
	ringBufferCapacity uint64

	sync.RWMutex
}

func (ec *EventSystemImpl) CreateEventStream(name string, count uint64) *EventStream {
	return ec.streaming.CreateEventStream(name, count)
}

func (ec *EventSystemImpl) RemoveStream(consumer *EventStream) {
	ec.streaming.RemoveEventStream(consumer)
}

func (ec *EventSystemImpl) GetEventsFromID(id, count uint64) ([]*si.EventRecord, uint64, uint64) {
	return ec.eventBuffer.GetEventsFromID(id, count)
}

func GetEventSystem() EventSystem {
	once.Do(func() {
		Init()
	})
	return ev
}

func (ec *EventSystemImpl) IsEventTrackingEnabled() bool {
	ec.RLock()
	defer ec.RUnlock()
	return ec.trackingEnabled
}

func (ec *EventSystemImpl) GetRequestCapacity() int {
	ec.RLock()
	defer ec.RUnlock()
	return ec.requestCapacity
}

func (ec *EventSystemImpl) GetRingBufferCapacity() uint64 {
	ec.RLock()
	defer ec.RUnlock()
	return ec.ringBufferCapacity
}

// VisibleForTesting
func Init() {
	store := newEventStore()
	buffer := newEventRingBuffer(defaultRingBufferSize)
	ev = &EventSystemImpl{
		Store:         store,
		channel:       make(chan *si.EventRecord, defaultEventChannelSize),
		stop:          make(chan bool),
		stopped:       false,
		publisher:     CreateShimPublisher(store),
		eventBuffer:   buffer,
		eventSystemId: fmt.Sprintf("event-system-%d", time.Now().Unix()),
		streaming:     NewEventStreaming(buffer),
	}
}

func (ec *EventSystemImpl) StartService() {
	ec.StartServiceWithPublisher(true)
}

func (ec *EventSystemImpl) StartServiceWithPublisher(withPublisher bool) {
	ec.Lock()
	defer ec.Unlock()

	configs.AddConfigMapCallback(ec.eventSystemId, func() {
		go ec.reloadConfig()
	})

	ec.trackingEnabled = ec.readIsTrackingEnabled()
	ec.ringBufferCapacity = ec.readRingBufferCapacity()
	ec.requestCapacity = ec.readRequestCapacity()

	go func() {
		log.Log(log.Events).Info("Starting event system handler")
		for {
			select {
			case <-ec.stop:
				return
			case event, ok := <-ec.channel:
				if !ok {
					return
				}
				if event != nil {
					ec.Store.Store(event)
					ec.eventBuffer.Add(event)
					ec.streaming.PublishEvent(event)
					metrics.GetEventMetrics().IncEventsProcessed()
				}
			}
		}
	}()
	if withPublisher {
		ec.publisher.StartService()
	}
}

func (ec *EventSystemImpl) Stop() {
	ec.Lock()
	defer ec.Unlock()

	configs.RemoveConfigMapCallback(ec.eventSystemId)

	if ec.stopped {
		return
	}

	ec.stop <- true
	if ec.channel != nil {
		close(ec.channel)
		ec.channel = nil
	}
	ec.publisher.Stop()
	ec.stopped = true
}

func (ec *EventSystemImpl) AddEvent(event *si.EventRecord) {
	metrics.GetEventMetrics().IncEventsCreated()
	select {
	case ec.channel <- event:
		metrics.GetEventMetrics().IncEventsChanneled()
	default:
		log.Log(log.Events).Debug("could not add Event to channel")
		metrics.GetEventMetrics().IncEventsNotChanneled()
	}
}

func (ec *EventSystemImpl) readIsTrackingEnabled() bool {
	return common.GetConfigurationBool(configs.GetConfigMap(), configs.CMEventTrackingEnabled, configs.DefaultEventTrackingEnabled)
}

func (ec *EventSystemImpl) readRequestCapacity() int {
	return common.GetConfigurationInt(configs.GetConfigMap(), configs.CMEventRequestCapacity, configs.DefaultEventRequestCapacity)
}

func (ec *EventSystemImpl) readRingBufferCapacity() uint64 {
	return common.GetConfigurationUint(configs.GetConfigMap(), configs.CMEventRingBufferCapacity, configs.DefaultEventRingBufferCapacity)
}

func (ec *EventSystemImpl) isRestartNeeded() bool {
	ec.RLock()
	defer ec.RUnlock()
	return ec.readIsTrackingEnabled() != ec.trackingEnabled
}

func (ec *EventSystemImpl) Restart() {
	ec.Stop()
	ec.StartServiceWithPublisher(true)
}

// VisibleForTesting
func (ec *EventSystemImpl) CloseAllStreams() {
	ec.streaming.Lock()
	defer ec.streaming.Unlock()
	for consumer := range ec.streaming.eventStreams {
		ec.streaming.removeEventStream(consumer)
	}
}

func (ec *EventSystemImpl) reloadConfig() {
	ec.updateRequestCapacity()

	// resize the ring buffer with new capacity
	ec.eventBuffer.Resize(ec.readRingBufferCapacity())

	if ec.isRestartNeeded() {
		ec.Restart()
	}
}

func (ec *EventSystemImpl) updateRequestCapacity() {
	ec.Lock()
	defer ec.Unlock()
	ec.requestCapacity = ec.readRequestCapacity()
}
