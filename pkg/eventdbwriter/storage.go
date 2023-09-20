package eventdbwriter

import (
	"database/sql"
	"errors"
	"log"
	"sync/atomic"

	"gorm.io/gorm"

	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

type DBInfo struct {
	User     string
	Password string
	Host     string
	Port     int
	Name     string
}

type Storage interface {
	PersistEvents(startEventID uint64, events []*si.EventRecord) error
	GetAllEventsForApp(appID string) ([]*si.EventRecord, error)
	SetYunikornID(yunikornID string)
}

type DBStorage struct {
	db         *gorm.DB
	yunikornID atomic.Value
}

func (s *DBStorage) SetYunikornID(yunikornID string) {
	s.yunikornID.Store(yunikornID)
}

func (s *DBStorage) PersistEvents(startEventID uint64, events []*si.EventRecord) error {
	yunikornID, ok := s.yunikornID.Load().(string)
	if !ok {
		return errors.New("yunikorn instance ID is not set")
	}
	err := s.db.Transaction(func(tx *gorm.DB) error {
		i := startEventID
		for _, event := range events {
			txErr := tx.Create(entryFromSI(yunikornID, i, event)).Error
			if txErr != nil {
				return txErr
			}
			i++
		}
		return nil
	}, &sql.TxOptions{
		Isolation: sql.LevelReadCommitted,
		ReadOnly:  false,
	})

	return err
}

func (s *DBStorage) GetAllEventsForApp(appID string) ([]*si.EventRecord, error) {
	yunikornID, ok := s.yunikornID.Load().(string)
	if !ok {
		return nil, errors.New("yunikorn instance ID is not set")
	}
	var result []EventDBEntry
	err := s.db.Transaction(func(tx *gorm.DB) error {
		return s.db.Where("objectID = ? AND yunikorn_id = ?", appID, yunikornID).Find(&result).Error
	})
	if err != nil {
		log.Printf("ERROR: could not read from database: %v", err)
		return nil, err
	}

	records := make([]*si.EventRecord, 0, len(result))
	for _, dbEntry := range result {
		record := siFromEntry(dbEntry)
		records = append(records, record)
	}

	return records, nil
}
