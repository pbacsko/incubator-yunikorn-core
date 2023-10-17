package eventdbwriter

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/julienschmidt/httprouter"
	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

func TestNoAppID(t *testing.T) {
	recorder := httptest.NewRecorder()
	web := NewWebService(NewEventCache(), NewMockDB())

	web.GetAppEvents(recorder, nil, httprouter.Params{})

	assert.Equal(t, http.StatusBadRequest, recorder.Code)
	checkResponse(t, "application id is undefined", recorder)
}

func TestHistoryNotInCache(t *testing.T) {
	recorder := httptest.NewRecorder()
	req := getRequest()
	cache := NewEventCache()
	mockDB := NewMockDB()
	web := NewWebService(cache, mockDB)

	mockDB.setEvents([]*si.EventRecord{
		{TimestampNano: 123, ObjectID: "app-1"},
		{TimestampNano: 234, ObjectID: "app-1"},
	})

	web.GetAppEvents(recorder, req, httprouter.Params{
		{
			Key:   "appId",
			Value: "app-1",
		},
	})
	assert.Equal(t, http.StatusOK, recorder.Code)
	checkEvents(t, recorder)
	assert.Equal(t, 2, len(cache.GetEvents("app-1")))
}

func TestNoEventsForApp(t *testing.T) {
	recorder := httptest.NewRecorder()
	req := getRequest()
	cache := NewEventCache()
	mockDB := NewMockDB()
	web := NewWebService(cache, mockDB)

	web.GetAppEvents(recorder, req, httprouter.Params{
		{
			Key:   "appId",
			Value: "app-1",
		},
	})

	assert.Equal(t, http.StatusOK, recorder.Code)
	checkResponse(t, "\"Events\":null", recorder)
}

func TestHistoryIsCached(t *testing.T) {
	recorder := httptest.NewRecorder()
	req := getRequest()
	cache := NewEventCache()
	mockDB := NewMockDB()
	web := NewWebService(cache, mockDB)

	cache.events = map[string][]*si.EventRecord{
		"app-1": {
			{TimestampNano: 123, ObjectID: "app-1"},
			{TimestampNano: 234, ObjectID: "app-1"},
		},
	}
	cache.fullHistory = map[string]bool{
		"app-1": true,
	}
	web.GetAppEvents(recorder, req, httprouter.Params{
		{
			Key:   "appId",
			Value: "app-1",
		},
	})
	assert.Equal(t, http.StatusOK, recorder.Code)
	checkEvents(t, recorder)
}

func TestBackendFailure(t *testing.T) {
	recorder := httptest.NewRecorder()
	req := getRequest()
	cache := NewEventCache()
	mockDB := NewMockDB()
	mockDB.setDbFetchFailure(true)
	web := NewWebService(cache, mockDB)

	web.GetAppEvents(recorder, req, httprouter.Params{
		{
			Key:   "appId",
			Value: "app-1",
		},
	})
	assert.Equal(t, http.StatusBadRequest, recorder.Code)
	checkResponse(t, "Could not retrieve events from backend storage", recorder)
}

func TestWebServiceBackground(t *testing.T) {
	cache := NewEventCache()
	mockDB := NewMockDB()
	web := NewWebService(cache, mockDB)

	mockDB.setEvents([]*si.EventRecord{
		{TimestampNano: 123, ObjectID: "app-1"},
		{TimestampNano: 234, ObjectID: "app-1"},
	})

	ctx, cancel := context.WithCancel(context.Background())
	web.Start(ctx)
	// to detect that shutdown was initiated properly
	var shutdown atomic.Bool
	shutdownSig := make(chan struct{})
	web.httpServer.RegisterOnShutdown(func() {
		shutdown.Store(true)
		shutdownSig <- struct{}{}
	})

	// retrieve events using mock DB
	time.Sleep(100 * time.Millisecond)
	result, err := fetchFromRESTendpoint("app-1")
	assert.NilError(t, err)
	assert.Equal(t, 2, len(result.Events))
	assert.Equal(t, int64(123), result.Events[0].TimestampNano)
	assert.Equal(t, int64(234), result.Events[1].TimestampNano)

	// check cancellation
	cancel()
	select {
	case <-shutdownSig:
		break
	case <-time.After(time.Second):
		break
	}
	assert.Assert(t, shutdown.Load(), "shutdown was not initiated by cancel()")
}

func checkEvents(t *testing.T, recorder *httptest.ResponseRecorder) {
	body := make([]byte, 1024)
	n, err := recorder.Result().Body.Read(body)
	assert.NilError(t, err)
	var response QueryResponse
	err = json.Unmarshal(body[:n], &response)
	events := response.Events
	assert.NilError(t, err)
	assert.Equal(t, 2, len(events))
	assert.Equal(t, int64(123), events[0].TimestampNano)
	assert.Equal(t, "app-1", events[0].ObjectID)
	assert.Equal(t, int64(234), events[1].TimestampNano)
	assert.Equal(t, "app-1", events[1].ObjectID)
}

func checkResponse(t *testing.T, expected string, recorder *httptest.ResponseRecorder) {
	body := make([]byte, 1024)
	n, err := recorder.Result().Body.Read(body)
	assert.NilError(t, err)
	assert.Assert(t, strings.Contains(string(body[:n]), expected))
}

func getRequest() *http.Request {
	req, err := http.NewRequestWithContext(context.Background(), "GET", "ignore", &bufio.Reader{})
	if err != nil {
		panic(err)
	}
	return req
}

func fetchFromRESTendpoint(appID string) (*QueryResponse, error) {
	var buf io.ReadWriter
	req, err := http.NewRequest("GET", "http://localhost:9111/ws/v1/appevents/"+appID, buf)
	if err != nil {
		return nil, err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var result QueryResponse
	err = json.NewDecoder(resp.Body).Decode(&result)
	return &result, err
}
