package eventdbwriter

import (
	"context"
	"encoding/json"
	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
	"gotest.tools/v3/assert"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/julienschmidt/httprouter"

	"github.com/apache/yunikorn-core/pkg/webservice/dao"
)

type handlerMode int

const (
	sendBadRequest handlerMode = iota
	sendInternalServerError
	sendTrackingNotEnabled
	sendNormalResponse
)

func TestHttpClient(t *testing.T) {
	handler := &requestHandler{t: t}
	httpServer := setupWebServer(handler)
	defer httpServer.Close()
	go func() {
		httpServer.ListenAndServe()
	}()
	time.Sleep(100 * time.Millisecond)

	// send Bad request (no payload)
	handler.setMode(sendBadRequest)
	client := NewHttpClient("localhost:9998")
	response, err := client.GetRecentEvents(context.Background(), 0)
	assert.ErrorContains(t, err, "unexpected HTTP status code 400")
	assert.Assert(t, response.ykError == nil)
	assert.Assert(t, response.eventRecord == nil)

	// send Internal server error (no payload)
	handler.setMode(sendInternalServerError)
	client = NewHttpClient("localhost:9998")
	response, err = client.GetRecentEvents(context.Background(), 0)
	assert.ErrorContains(t, err, "unexpected HTTP status code 500")
	assert.Assert(t, response.ykError == nil)
	assert.Assert(t, response.eventRecord == nil)

	// send YK error back
	handler.setMode(sendTrackingNotEnabled)
	client = NewHttpClient("localhost:9998")
	response, err = client.GetRecentEvents(context.Background(), 0)
	assert.Assert(t, response.ykError != nil)
	assert.Assert(t, strings.Contains(response.ykError.Description, "Event tracking is disabled"))
	assert.Assert(t, strings.Contains(response.ykError.Description, "Event tracking is disabled"))
	assert.Equal(t, http.StatusBadRequest, response.ykError.StatusCode)
	assert.Assert(t, response.eventRecord == nil)

	// send normal response
	handler.setMode(sendNormalResponse)
	client = NewHttpClient("localhost:9998")
	response, err = client.GetRecentEvents(context.Background(), 0)
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

func setupWebServer(handler *requestHandler) *http.Server {
	router := httprouter.New()
	router.Handler("GET", "/ws/v1/events/batch", handler)
	httpServer := &http.Server{Addr: ":9998", Handler: router, ReadHeaderTimeout: time.Second}
	return httpServer
}

type requestHandler struct {
	mode handlerMode
	t    *testing.T

	sync.Mutex
}

func (h *requestHandler) setMode(m handlerMode) {
	h.Lock()
	defer h.Unlock()
	h.mode = m
}

func (h *requestHandler) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	switch h.mode {
	case sendInternalServerError:
		h.sendInternalServerError(w)
		return
	case sendBadRequest:
		h.sendBadRequest(w)
		return
	case sendNormalResponse:
		h.sendNormalResponse(w)
		return
	case sendTrackingNotEnabled:
		h.sendTrackingNotEnabled(w)
		return
	}
}

func (h *requestHandler) sendInternalServerError(w http.ResponseWriter) {
	h.writeHeaders(w)
	w.WriteHeader(http.StatusInternalServerError)
}

func (h *requestHandler) sendBadRequest(w http.ResponseWriter) {
	h.writeHeaders(w)
	w.WriteHeader(http.StatusBadRequest)
}

func (h *requestHandler) sendNormalResponse(w http.ResponseWriter) {
	h.writeHeaders(w)
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(dao.EventRecordDAO{
		InstanceUUID: "uuid-0",
		LowestID:     100,
		HighestID:    200,
		EventRecords: []*si.EventRecord{
			{TimestampNano: 123, Type: si.EventRecord_APP},
			{TimestampNano: 456, Type: si.EventRecord_NODE},
		},
	}); err != nil {
		h.t.Fatalf("%v", err)
	}
}

func (h *requestHandler) sendTrackingNotEnabled(w http.ResponseWriter) {
	h.writeHeaders(w)
	code := http.StatusBadRequest
	w.WriteHeader(code)
	errorInfo := dao.NewYAPIError(nil, code, "Event tracking is disabled")
	if err := json.NewEncoder(w).Encode(errorInfo); err != nil {
		h.t.Fatalf("%v", err)
	}
}

func (h *requestHandler) writeHeaders(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Credentials", "true")
	w.Header().Set("Access-Control-Allow-Methods", "GET,POST,HEAD,OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "X-Requested-With,Content-Type,Accept,Origin")
}
