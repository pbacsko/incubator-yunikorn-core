package eventdbwriter

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/julienschmidt/httprouter"
	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-core/pkg/webservice/dao"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

func TestHttpClient(t *testing.T) {
	handler := &requestHandler{}
	httpServer := setupWebServer(handler)
	defer httpServer.Close()
	go func() {
		httpServer.ListenAndServe() //nolint:errcheck
	}()
	time.Sleep(100 * time.Millisecond)

	// send Bad request (no payload)
	handler.setHandler(func(w http.ResponseWriter, _ *http.Request) {
		writeHeaders(w)
		w.WriteHeader(http.StatusBadRequest)
	})
	client := NewHttpClient("localhost:9080")
	response, err := client.GetRecentEvents(context.Background(), 0)
	assert.ErrorContains(t, err, "unexpected HTTP status code 400")
	assert.Assert(t, response.ykError == nil)
	assert.Assert(t, response.eventRecord == nil)

	// send Internal server error (no payload)
	handler.setHandler(func(w http.ResponseWriter, _ *http.Request) {
		writeHeaders(w)
		w.WriteHeader(http.StatusInternalServerError)
	})
	client = NewHttpClient("localhost:9080")
	response, err = client.GetRecentEvents(context.Background(), 0)
	assert.ErrorContains(t, err, "unexpected HTTP status code 500")
	assert.Assert(t, response.ykError == nil)
	assert.Assert(t, response.eventRecord == nil)

	// send YK error back
	handler.setHandler(func(w http.ResponseWriter, _ *http.Request) {
		writeHeaders(w)
		code := http.StatusBadRequest
		w.WriteHeader(code)
		errorInfo := dao.NewYAPIError(nil, code, "Event tracking is disabled")
		if jsonErr := json.NewEncoder(w).Encode(errorInfo); jsonErr != nil {
			panic(jsonErr)
		}
	})
	client = NewHttpClient("localhost:9080")
	response, err = client.GetRecentEvents(context.Background(), 0)
	assert.ErrorContains(t, err, "error received from Yunikorn: Event tracking is disabled")
	assert.Assert(t, response.ykError != nil)
	assert.Assert(t, strings.Contains(response.ykError.Description, "Event tracking is disabled"))
	assert.Assert(t, strings.Contains(response.ykError.Description, "Event tracking is disabled"))
	assert.Equal(t, http.StatusBadRequest, response.ykError.StatusCode)
	assert.Assert(t, response.eventRecord == nil)

	// send normal response
	handler.setHandler(func(w http.ResponseWriter, _ *http.Request) {
		writeHeaders(w)
		w.WriteHeader(http.StatusOK)
		if jsonErr := json.NewEncoder(w).Encode(dao.EventRecordDAO{
			InstanceUUID: "uuid-0",
			LowestID:     100,
			HighestID:    200,
			EventRecords: []*si.EventRecord{
				{TimestampNano: 123, Type: si.EventRecord_APP},
				{TimestampNano: 456, Type: si.EventRecord_NODE},
			},
		}); jsonErr != nil {
			panic(jsonErr)
		}
	})
	client = NewHttpClient("localhost:9080")
	response, err = client.GetRecentEvents(context.Background(), 0)
	assert.NilError(t, err)
	assert.Assert(t, response.eventRecord != nil)
	assert.Equal(t, uint64(100), response.eventRecord.LowestID)
	assert.Equal(t, uint64(200), response.eventRecord.HighestID)
	assert.Equal(t, "uuid-0", response.eventRecord.InstanceUUID)
	assert.Equal(t, 2, len(response.eventRecord.EventRecords))
	assert.Equal(t, si.EventRecord_APP, response.eventRecord.EventRecords[0].Type)
	assert.Equal(t, int64(123), response.eventRecord.EventRecords[0].TimestampNano)
	assert.Equal(t, si.EventRecord_NODE, response.eventRecord.EventRecords[1].Type)
	assert.Equal(t, int64(456), response.eventRecord.EventRecords[1].TimestampNano)
}

func setupWebServer(handler http.Handler) *http.Server {
	router := httprouter.New()
	router.Handler("GET", "/ws/v1/events/batch", handler)
	httpServer := &http.Server{Addr: ":9080", Handler: router, ReadHeaderTimeout: time.Second}
	return httpServer
}

type requestHandler struct {
	handler func(w http.ResponseWriter, r *http.Request)
	sync.Mutex
}

func (h *requestHandler) setHandler(f func(w http.ResponseWriter, r *http.Request)) {
	h.Lock()
	defer h.Unlock()
	h.handler = f
}

func (h *requestHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	fn := h.getHandler()
	if fn == nil {
		panic("handler function is unset")
	}
	fn(w, r)
}

func (h *requestHandler) getHandler() func(w http.ResponseWriter, r *http.Request) {
	h.Lock()
	defer h.Unlock()
	return h.handler
}
