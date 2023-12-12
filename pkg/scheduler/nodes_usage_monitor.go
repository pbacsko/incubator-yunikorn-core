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

package scheduler

import (
	"time"

	"github.com/apache/yunikorn-core/pkg/log"
	"github.com/apache/yunikorn-core/pkg/metrics"
)

type nodesResourceUsageMonitor struct {
	done   chan struct{}
	ticker *time.Ticker
	cc     *ClusterContext
}

func newNodesResourceUsageMonitor(scheduler *ClusterContext) *nodesResourceUsageMonitor {
	return &nodesResourceUsageMonitor{
		done:   make(chan struct{}),
		ticker: time.NewTicker(1 * time.Second),
		cc:     scheduler,
	}
}

func (m *nodesResourceUsageMonitor) start() {
	log.Log(log.SchedNodesUsage).Info("Starting node resource monitor")
	go func() {
		for {
			select {
			case <-m.done:
				m.ticker.Stop()
				return
			case <-m.ticker.C:
				m.runOnce()
			}
		}
	}()
}

func (m *nodesResourceUsageMonitor) runOnce() {
	for _, p := range m.cc.GetPartitionMapClone() {
		usageMap := p.calculateNodesResourceUsage()
		if len(usageMap) > 0 {
			for resourceName, usageBuckets := range usageMap {
				for idx, bucketValue := range usageBuckets {
					metrics.GetSchedulerMetrics().SetNodeResourceUsage(resourceName, idx, float64(bucketValue))
				}
			}
		}
	}
}

// Stop the node usage monitor.
func (m *nodesResourceUsageMonitor) stop() {
	log.Log(log.SchedNodesUsage).Info("Stopping node resource usage monitor")
	close(m.done)
}
