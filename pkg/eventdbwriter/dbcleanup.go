package eventdbwriter

import (
	"context"
	"time"

	"go.uber.org/zap"
)

const defaultCleanupPeriod = time.Hour

var cleanupPeriod = defaultCleanupPeriod

type DBCleaner struct {
	storage Storage
}

func (c *DBCleaner) Start(ctx context.Context) {
	go func(p time.Duration) {
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(p):
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
	}(cleanupPeriod)
}

func (c *DBCleaner) removeRows(ctx context.Context) (int64, error) {
	cutoff := time.Now().Add(-24 * time.Hour)
	GetLogger().Info("Cleaning up database - removing entries that are considered old",
		zap.Time("cutoff time", cutoff))
	numRemoved, err := c.storage.RemoveOldEntries(ctx, cutoff)
	if err != nil {
		return 0, err
	}
	return numRemoved, nil
}
