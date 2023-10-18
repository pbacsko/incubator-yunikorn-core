package eventdbwriter

import (
	"context"
	"database/sql"
	"errors"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
	"gorm.io/gorm"

	"github.com/apache/yunikorn-scheduler-interface/lib/go/si"
)

type Driver string

const (
	MySQL    Driver = "mysql"
	Postgres Driver = "postgres"
)

type DBInfo struct {
	User     string
	Password string
	Host     string
	Port     int
	Name     string
	Driver   Driver
}

type Storage interface {
	PersistEvents(ctx context.Context, startEventID uint64, events []*si.EventRecord) error
	GetAllEventsForApp(ctx context.Context, appID string) ([]*si.EventRecord, error)
	SetYunikornID(yunikornID string)
	RemoveObsoleteEntries(ctx context.Context, cutoff time.Time) (int64, error)
}

type DBStorage struct {
	db         *gorm.DB
	yunikornID atomic.Value
}

func NewDBStorage(db *gorm.DB) *DBStorage {
	return &DBStorage{
		db: db,
	}
}

func (s *DBStorage) SetYunikornID(yunikornID string) {
	s.yunikornID.Store(yunikornID)
}

func (s *DBStorage) PersistEvents(ctx context.Context, startEventID uint64, events []*si.EventRecord) error {
	yunikornID, ok := s.yunikornID.Load().(string)
	if !ok {
		return errors.New("yunikorn instance ID is not set")
	}
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
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

func (s *DBStorage) RemoveObsoleteEntries(ctx context.Context, cutoff time.Time) (int64, error) {
	var rowsAffected int64
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		result := tx.Where("event_time <= ?", cutoff).Delete(&EventDBEntry{})
		rowsAffected = result.RowsAffected
		return result.Error
	})

	return rowsAffected, err
}

func (s *DBStorage) GetAllEventsForApp(ctx context.Context, appID string) ([]*si.EventRecord, error) {
	yunikornID, ok := s.yunikornID.Load().(string)
	if !ok {
		return nil, errors.New("yunikorn instance ID is not set")
	}
	var result []EventDBEntry
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		return s.db.Where("object_id = ? AND yunikorn_id = ?", appID, yunikornID).Find(&result).Error
	})
	if err != nil {
		GetLogger().Error("Could not read from database", zap.Error(err))
		return nil, err
	}

	records := make([]*si.EventRecord, 0, len(result))
	for _, dbEntry := range result {
		record := siFromEntry(dbEntry)
		records = append(records, record)
	}

	return records, nil
}
