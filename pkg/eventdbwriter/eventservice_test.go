package eventdbwriter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"sync"
	"testing"
	"time"

	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-core/pkg/webservice/dao"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"

	_ "github.com/proullon/ramsql/driver"
)

// full integration test with in-memory SQL and Yunikorn REST API mock
func TestEventService(t *testing.T) {
	handler := &testHandler{}
	httpServer := setupWebServer(handler)
	defer httpServer.Close()
	go func() {
		httpServer.ListenAndServe() //nolint:errcheck
	}()
	events := []*si.EventRecord{
		{TimestampNano: 100, Type: si.EventRecord_APP, ObjectID: "app-1", EventChangeType: si.EventRecord_ADD},
		{TimestampNano: 123, Type: si.EventRecord_APP, ObjectID: "app-1", EventChangeType: si.EventRecord_SET, EventChangeDetail: si.EventRecord_APP_NEW},
		{TimestampNano: 200, Type: si.EventRecord_APP, ObjectID: "app-1", EventChangeType: si.EventRecord_SET, EventChangeDetail: si.EventRecord_APP_STARTING},
		{TimestampNano: 456, Type: si.EventRecord_NODE, ObjectID: "node-1"},
	}
	handler.setContents("yk-0", 0, 3, events)

	eventService := CreateEventService(DBInfo{Driver: RamSQL}, "localhost:9080")
	ctx, cancel := context.WithCancel(context.Background())
	eventService.Start(ctx)
	defer cancel()

	// wait for the persistence of the two initial events
	time.Sleep(2 * time.Second)
	resp, err := fetchFromRESTendpoint("app-1")
	assert.NilError(t, err)
	assert.Equal(t, 3, len(resp.Events))
	assert.NilError(t, handler.getTestError())

	// add two extra events
	events = make([]*si.EventRecord, 2)
	events[0] = &si.EventRecord{
		TimestampNano: 500, Type: si.EventRecord_APP, ObjectID: "app-2", EventChangeType: si.EventRecord_ADD,
	}
	events[1] = &si.EventRecord{
		TimestampNano: 600, Type: si.EventRecord_APP, ObjectID: "app-1", EventChangeType: si.EventRecord_SET, EventChangeDetail: si.EventRecord_APP_RUNNING,
	}

	handler.setContents("yk-0", 0, 5, events)
	time.Sleep(2 * time.Second)
	assert.NilError(t, handler.getTestError())
	resp, err = fetchFromRESTendpoint("app-1")
	assert.NilError(t, err)
	assert.Equal(t, 4, len(resp.Events))
	resp, err = fetchFromRESTendpoint("app-2")
	assert.NilError(t, err)
	assert.Equal(t, 1, len(resp.Events))

	// simulate YK restart
	httpServer.Close()
	time.Sleep(time.Second)
	httpServer = setupWebServer(handler)
	handler.setContents("yk-1", 0, 1, []*si.EventRecord{
		{TimestampNano: 100, Type: si.EventRecord_QUEUE, ObjectID: "root", EventChangeType: si.EventRecord_ADD},
		{TimestampNano: 150, Type: si.EventRecord_NODE, ObjectID: "node-1", EventChangeType: si.EventRecord_ADD},
	})
	defer httpServer.Close()
	go func() {
		httpServer.ListenAndServe() //nolint:errcheck
	}()
	time.Sleep(2 * time.Second)
	// no events expected for "app-1" and "app-2" due to restart
	resp, err = fetchFromRESTendpoint("app-1")
	assert.NilError(t, err)
	assert.Equal(t, 0, len(resp.Events))
	resp, err = fetchFromRESTendpoint("app-2")
	assert.NilError(t, err)
	assert.Equal(t, 0, len(resp.Events))
}

type testHandler struct {
	err     error
	ykID    string
	lowest  uint64
	highest uint64
	events  []*si.EventRecord

	sync.Mutex
}

func (th *testHandler) getTestError() error {
	th.Lock()
	defer th.Unlock()
	return th.err
}

func (th *testHandler) setContents(ykID string, lowest, highest uint64, events []*si.EventRecord) {
	th.Lock()
	defer th.Unlock()
	th.ykID = ykID
	th.lowest = lowest
	th.highest = highest
	th.events = events
}

func (th *testHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	th.Lock()
	defer th.Unlock()

	writeHeaders(w)
	w.WriteHeader(http.StatusOK)
	var startStr string
	if startStr = r.URL.Query().Get("start"); startStr == "" {
		th.err = errors.New("no start defined in the URL query")
		return
	}
	start, err := strconv.ParseUint(startStr, 10, 64)
	if err != nil {
		th.err = err
		return
	}

	if start == 0 {
		if err = json.NewEncoder(w).Encode(getEventRecordDAO(th.ykID, th.lowest, th.highest, th.events)); err != nil {
			th.err = err
		}
		return
	}

	// send empty response when polling for new changes (and there's none) & initial request
	if start == uint64(len(th.events)) || start == math.MaxUint64 {
		if err = json.NewEncoder(w).Encode(getEventRecordDAO(th.ykID, th.lowest, th.highest, nil)); err != nil {
			th.err = err
		}
		return
	}

	// two new events case
	if start == 4 && len(th.events) == 2 {
		if err = json.NewEncoder(w).Encode(getEventRecordDAO(th.ykID, th.lowest, th.highest, th.events)); err != nil {
			th.err = err
		}
		return
	}

	// after reading the two new events
	if start == 6 {
		if err = json.NewEncoder(w).Encode(getEventRecordDAO(th.ykID, th.lowest, th.highest, nil)); err != nil {
			th.err = err
		}
		return
	}

	th.err = fmt.Errorf("illegal start received in the URL query: %d", start)
}

func getEventRecordDAO(ykID string, lowest, highest uint64, events []*si.EventRecord) dao.EventRecordDAO {
	return dao.EventRecordDAO{
		InstanceUUID: ykID,
		LowestID:     lowest,
		HighestID:    highest,
		EventRecords: events,
	}
}
