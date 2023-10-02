package eventdbwriter

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strconv"

	"github.com/apache/yunikorn-core/pkg/webservice/dao"
)

const eventPath = "/ws/v1/events/batch"

type YunikornClient interface {
	GetRecentEvents(startID uint64) (*dao.EventRecordDAO, error)
}

type HttpClient struct {
	host string
}

func NewHttpClient(host string) *HttpClient {
	return &HttpClient{
		host: host,
	}
}

func (h *HttpClient) GetRecentEvents(startID uint64) (*dao.EventRecordDAO, error) {
	req, err := h.newRequest(startID)
	req = req.WithContext(context.Background())
	if err != nil {
		return nil, err
	}
	var events dao.EventRecordDAO
	_, err = h.do(req, &events)

	return &events, err
}

func (h *HttpClient) newRequest(startID uint64) (*http.Request, error) {
	rel := &url.URL{Path: eventPath}
	wsUrl := &url.URL{
		Host:   h.host,
		Scheme: "http",
	}
	u := wsUrl.ResolveReference(rel)
	u.RawQuery = "start=" + strconv.FormatUint(startID, 10)
	var buf io.ReadWriter
	req, err := http.NewRequest("GET", u.String(), buf)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "Golang_Spider_Bot/3.0")
	return req, nil
}

func (h *HttpClient) do(req *http.Request, v interface{}) (*http.Response, error) {
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	err = json.NewDecoder(resp.Body).Decode(v)
	return resp, err
}
