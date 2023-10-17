package eventdbwriter

import (
	"context"
	"gotest.tools/v3/assert"
	"testing"
	"time"
)

func TestRemoveRows(t *testing.T) {
	mockDB := NewMockDB()
	cleaner := &DBCleaner{
		storage: mockDB,
	}
	mockDB.numRemoved = 15

	numRemoved, err := cleaner.removeRows(context.Background())

	removes := mockDB.getRemoveCalls()
	assert.Equal(t, 1, len(removes))
	assert.Equal(t, int64(15), numRemoved)
	assert.NilError(t, err)
	diff := time.Now().Sub(removes[0].cutoff).Round(time.Hour)
	assert.Equal(t, 24*time.Hour, diff)
}

func TestRemoveRowsError(t *testing.T) {
	mockDB := NewMockDB()
	cleaner := &DBCleaner{
		storage: mockDB,
	}
	mockDB.removeFails = true

	numRemoved, err := cleaner.removeRows(context.Background())

	assert.Equal(t, 1, len(mockDB.getRemoveCalls()))
	assert.Equal(t, int64(0), numRemoved)
	assert.ErrorContains(t, err, "error while removing records")
}

func TestRemoveRecordsBackground(t *testing.T) {
	cleanupPeriod = time.Millisecond * 100
	defer func() {
		cleanupPeriod = defaultCleanupPeriod
	}()
	mockDB := NewMockDB()
	cleaner := &DBCleaner{
		storage: mockDB,
	}
	mockDB.numRemoved = 15

	ctx, cancel := context.WithCancel(context.Background())
	cleaner.Start(ctx)
	time.Sleep(time.Second)
	cancel()

	removes := mockDB.getRemoveCalls()
	assert.Assert(t, len(removes) >= 9 && len(removes) <= 11, "expected to have 9-11 remove calls, got %d", mockDB.removeCalls)
}
