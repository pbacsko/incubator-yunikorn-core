package eventdbwriter

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

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
	storage := NewMockDB()
	web := NewWebService(cache, storage)

	storage.events = []*si.EventRecord{
		{TimestampNano: 123, ObjectID: "app-1"},
		{TimestampNano: 234, ObjectID: "app-1"},
	}

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
	storage := NewMockDB()
	web := NewWebService(cache, storage)

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
	storage := NewMockDB()
	web := NewWebService(cache, storage)

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
	storage := NewMockDB()
	storage.getEventsFails = true
	web := NewWebService(cache, storage)

	web.GetAppEvents(recorder, req, httprouter.Params{
		{
			Key:   "appId",
			Value: "app-1",
		},
	})
	assert.Equal(t, http.StatusBadRequest, recorder.Code)
	checkResponse(t, "Could not retrieve events from backend storage", recorder)
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
