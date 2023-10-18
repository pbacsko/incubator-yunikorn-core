package eventdbwriter

import (
	"encoding/json"
	"time"

	"go.uber.org/zap"

	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

// EventDBEntry Database row object for GORM
type EventDBEntry struct {
	YunikornID   string `gorm:"primarykey;not null;size:36"`
	EventID      uint64 `gorm:"primarykey;not null"`
	Type         int32  `gorm:"not null"`
	ObjectID     string `gorm:"not null"`
	ReferenceID  string
	Message      string
	Timestamp    time.Time `gorm:"not null"`
	ChangeType   int32     `gorm:"not null"`
	ChangeDetail int32     `gorm:"not null"`
	Resource     string
}

func entryFromSI(yunikornID string, eventID uint64, event *si.EventRecord) *EventDBEntry {
	var resource string
	if event.Resource != nil {
		r, err := json.Marshal(event.Resource)
		if err != nil {
			GetLogger().Error("Unable to properly marshal resource object",
				zap.Any("resource", event.Resource))
		} else {
			resource = string(r)
		}
	}

	return &EventDBEntry{
		YunikornID:   yunikornID,
		EventID:      eventID,
		Type:         int32(event.Type),
		ObjectID:     event.ObjectID,
		ReferenceID:  event.ReferenceID,
		Message:      event.Message,
		Timestamp:    time.Unix(0, event.TimestampNano),
		ChangeType:   int32(event.EventChangeType),
		ChangeDetail: int32(event.EventChangeDetail),
		Resource:     resource,
	}
}

func siFromEntry(dbEntry EventDBEntry) *si.EventRecord {
	event := &si.EventRecord{
		Type:              si.EventRecord_Type(dbEntry.Type),
		ObjectID:          dbEntry.ObjectID,
		ReferenceID:       dbEntry.ReferenceID,
		Message:           dbEntry.Message,
		TimestampNano:     dbEntry.Timestamp.UnixNano(),
		EventChangeType:   si.EventRecord_ChangeType(dbEntry.ChangeType),
		EventChangeDetail: si.EventRecord_ChangeDetail(dbEntry.ChangeDetail),
	}

	if dbEntry.Resource != "" {
		var res si.Resource
		err := json.Unmarshal([]byte(dbEntry.Resource), &res)
		if err != nil {
			GetLogger().Error("Unable to unmarshal resources from database entry",
				zap.String("resource", dbEntry.Resource))
		} else {
			event.Resource = &res
		}
	}

	return event
}
