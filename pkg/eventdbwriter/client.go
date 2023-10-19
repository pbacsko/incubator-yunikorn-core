package eventdbwriter

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"

	"go.uber.org/zap"

	"github.com/apache/yunikorn-core/pkg/webservice/dao"
)

const eventPath = "/ws/v1/events/batch"

type YunikornClient interface {
	GetRecentEvents(ctx context.Context, startID uint64) (*EventQueryResult, error)
}

type EventQueryResult struct {
	eventRecord *dao.EventRecordDAO
	ykError     *dao.YAPIError
}

type HttpClient struct {
	host string
}

func NewHttpClient(host string) *HttpClient {
	return &HttpClient{
		host: host,
	}
}

func (h *HttpClient) GetRecentEvents(ctx context.Context, startID uint64) (*EventQueryResult, error) {
	req, err := h.newRequest(ctx, startID)
	if err != nil {
		return nil, err
	}

	events, ykErr, err := h.do(req)

	return &EventQueryResult{
		eventRecord: events,
		ykError:     ykErr,
	}, err
}

func (h *HttpClient) newRequest(ctx context.Context, startID uint64) (*http.Request, error) {
	rel := &url.URL{Path: eventPath}
	wsUrl := &url.URL{
		Host:   h.host,
		Scheme: "http",
	}
	u := wsUrl.ResolveReference(rel)
	u.RawQuery = "start=" + strconv.FormatUint(startID, 10)
	var buf io.ReadWriter
	req, err := http.NewRequestWithContext(ctx, "GET", u.String(), buf)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "Golang_Spider_Bot/3.0")
	return req, nil
}

func (h *HttpClient) do(req *http.Request) (*dao.EventRecordDAO, *dao.YAPIError, error) {
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()

	code := resp.StatusCode
	if code != http.StatusOK {
		GetLogger().Warn("HTTP status was not OK", zap.Int("code", code))
		// attempt to unmarshal body as a Yunikorn error object
		var ykErr dao.YAPIError
		err = json.NewDecoder(resp.Body).Decode(&ykErr)
		if err != nil {
			// make sure that an error is always returned
			return nil, nil, fmt.Errorf("unexpected HTTP status code %d", code)
		}
		return nil, &ykErr, fmt.Errorf("error received from Yunikorn: %s", ykErr.Message)
	}

	var eventRecord dao.EventRecordDAO
	err = json.NewDecoder(resp.Body).Decode(&eventRecord)
	if err != nil {
		return nil, nil, err
	}
	return &eventRecord, nil, nil
}
