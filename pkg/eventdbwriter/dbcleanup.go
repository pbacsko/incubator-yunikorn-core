package eventdbwriter

import (
	"context"
	"time"

	"go.uber.org/zap"
)

var cleanupPeriod = time.Hour

type DBCleaner struct {
	storage Storage
}

func (c *DBCleaner) Start(ctx context.Context) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(cleanupPeriod):
				c.removeRows(ctx)
			}
		}
	}()
}

func (c *DBCleaner) removeRows(ctx context.Context) {
	cutoff := time.Now().Add(-24 * time.Hour)
	GetLogger().Info("Cleaning up database - removing entries that are considered old",
		zap.Time("cutoff time", cutoff))
	numRemoved, err := c.storage.RemoveOldEntries(ctx, cutoff)
	if err != nil {
		GetLogger().Error("Error while removing database entries",
			zap.Error(err))
		return
	}
	if numRemoved > 0 {
		GetLogger().Info("Database cleanup performed", zap.Int64("number of deleted rows",
			numRemoved))
	}
}
