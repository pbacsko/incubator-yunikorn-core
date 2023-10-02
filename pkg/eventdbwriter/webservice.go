package eventdbwriter

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/julienschmidt/httprouter"
	"go.uber.org/zap"

	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

type WebService struct {
	httpServer *http.Server
	cache      *EventCache
	storage    Storage
}

type QueryResponse struct {
	Events []*si.EventRecord
}

type ErrorResponse struct {
	Message string
}

func (m *WebService) Start(ctx context.Context) {
	router := httprouter.New()
	router.Handle("GET", "/ws/v1/appevents/:appId", m.GetAppEvents)
	router.Handle("GET", "/ws/v1/appevents/:appId", func(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
		r = r.WithContext(ctx)
		m.GetAppEvents(w, r, params)
	})
	m.httpServer = &http.Server{Addr: ":9111", Handler: router, ReadHeaderTimeout: time.Second}

	GetLogger().Info("Starting web service")
	go func() {
		httpError := m.httpServer.ListenAndServe()
		if httpError != nil && httpError != http.ErrServerClosed {
			GetLogger().Fatal("HTTP serving error", zap.Error(httpError))
		}
	}()
	go func() {
		<-ctx.Done()
		m.httpServer.Close()
	}()
}

func (m *WebService) GetAppEvents(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	writeHeaders(w)
	appId := params.ByName("appId")
	if appId == "" {
		sendError(w, "application id is undefined")
		return
	}

	events := m.cache.GetEvents(appId)
	if events == nil {
		GetLogger().Info("Fetching events from backend for application",
			zap.String("appID", appId))
		var err error
		events, err = m.storage.GetAllEventsForApp(r.Context(), appId)
		if err != nil {
			GetLogger().Error("Could not retrieve events from backend storage",
				zap.Error(err))
			sendError(w, fmt.Sprintf("ERROR: Could not retrieve events from backend storage: %v", err))
			return
		}
		if len(events) == 0 {
			GetLogger().Info("No events for application", zap.String("appID", appId))
		} else {
			m.cache.AddEvents(appId, events)
			m.cache.SetHaveFullHistory(appId)
		}
	}

	err := json.NewEncoder(w).Encode(QueryResponse{
		Events: events,
	})
	if err != nil {
		GetLogger().Error("Could not marshall response", zap.Error(err))
	}
}

func sendError(w http.ResponseWriter, msg string) {
	w.WriteHeader(http.StatusBadRequest)
	err := json.NewEncoder(w).Encode(ErrorResponse{
		Message: msg,
	})
	if err != nil {
		GetLogger().Error("Could not marshall response", zap.Error(err))
	}
}

func writeHeaders(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Credentials", "true")
	w.Header().Set("Access-Control-Allow-Methods", "GET,POST,HEAD,OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "X-Requested-With,Content-Type,Accept,Origin")
}

func NewWebService(cache *EventCache, storage Storage) *WebService {
	w := &WebService{
		cache:   cache,
		storage: storage,
	}

	return w
}
