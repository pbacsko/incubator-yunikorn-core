package eventdbwriter

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/julienschmidt/httprouter"

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

func (m *WebService) Start() {
	router := httprouter.New()
	router.Handle("GET", "/ws/v1/appevents/:appId", m.GetAppEvents)
	m.httpServer = &http.Server{Addr: ":9111", Handler: router, ReadHeaderTimeout: time.Second}

	log.Println("Starting web service")
	go func() {
		httpError := m.httpServer.ListenAndServe()
		if httpError != nil && httpError != http.ErrServerClosed {
			log.Fatal("HTTP serving error", httpError)
		}
	}()
}

func (m *WebService) GetAppEvents(w http.ResponseWriter, _ *http.Request, params httprouter.Params) {
	writeHeaders(w)
	appId := params.ByName("appId")
	if appId == "" {
		sendError(w, "application id is undefined")
		return
	}

	events := m.cache.GetEvents(appId)
	if events == nil {
		log.Printf("Fetching events from backend for application %s", appId)
		var err error
		events, err = m.storage.GetAllEventsForApp(appId)
		if err != nil {
			msg := fmt.Sprintf("ERROR: Could not retrieve events from backend storage: %v", err)
			log.Println(msg)
			sendError(w, msg)
			return
		}
		if len(events) == 0 {
			log.Printf("No events for application %s", appId)
		} else {
			m.cache.AddEvents(appId, events)
			m.cache.SetHaveFullHistory(appId)
		}
	}

	err := json.NewEncoder(w).Encode(QueryResponse{
		Events: events,
	})
	if err != nil {
		log.Printf("ERROR: could not marshall response")
	}
}

func sendError(w http.ResponseWriter, msg string) {
	w.WriteHeader(http.StatusBadRequest)
	err := json.NewEncoder(w).Encode(ErrorResponse{
		Message: msg,
	})
	if err != nil {
		log.Printf("ERROR: could not marshall response")
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
