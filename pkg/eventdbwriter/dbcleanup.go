package eventdbwriter

import (
	"context"
	"time"

	"go.uber.org/zap"
)

const defaultCleanupPeriod = time.Hour

type DBCleaner struct {
	storage       Storage
	cleanupPeriod time.Duration
}

func NewDBCleaner(storage Storage) *DBCleaner {
	return &DBCleaner{
		storage:       storage,
		cleanupPeriod: defaultCleanupPeriod,
	}
}
func (c *DBCleaner) Start(ctx context.Context) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(c.cleanupPeriod):
				var numRemoved int64
				var err error
				if numRemoved, err = c.removeRows(ctx); err != nil {
					GetLogger().Error("Error while removing database entries",
						zap.Error(err))
					return
				}
				GetLogger().Info("Database cleanup performed", zap.Int64("number of deleted rows",
					numRemoved))
			}
		}
	}()
}

func (c *DBCleaner) removeRows(ctx context.Context) (int64, error) {
	cutoff := time.Now().Add(-24 * time.Hour)
	GetLogger().Info("Cleaning up database - removing entries that are considered obsolete",
		zap.Time("cutoff time", cutoff))
	numRemoved, err := c.storage.RemoveObsoleteEntries(ctx, cutoff)
	if err != nil {
		return 0, err
	}
	return numRemoved, nil
}
