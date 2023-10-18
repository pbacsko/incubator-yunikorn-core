package eventdbwriter

import (
	"context"
	"testing"
	"time"

	"gotest.tools/v3/assert"
)

func TestRemoveRows(t *testing.T) {
	mockDB := NewMockDB()
	cleaner := NewDBCleaner(mockDB)
	mockDB.setNumRemoved(15)

	numRemoved, err := cleaner.removeRows(context.Background())

	removes := mockDB.getRemoveCalls()
	assert.Equal(t, 1, len(removes))
	assert.Equal(t, int64(15), numRemoved)
	assert.NilError(t, err)
	diff := time.Since(removes[0].cutoff).Round(time.Hour)
	assert.Equal(t, 24*time.Hour, diff)
}

func TestRemoveRowsError(t *testing.T) {
	mockDB := NewMockDB()
	cleaner := NewDBCleaner(mockDB)
	mockDB.setDBFailure(true)

	numRemoved, err := cleaner.removeRows(context.Background())

	assert.Equal(t, 1, len(mockDB.getRemoveCalls()))
	assert.Equal(t, int64(0), numRemoved)
	assert.ErrorContains(t, err, "error while removing records")
}

func TestRemoveRecordsBackground(t *testing.T) {
	mockDB := NewMockDB()
	cleaner := NewDBCleaner(mockDB)
	cleaner.cleanupPeriod = 10 * time.Millisecond
	mockDB.setNumRemoved(15)

	ctx, cancel := context.WithCancel(context.Background())
	cleaner.Start(ctx)

	time.Sleep(100 * time.Millisecond)
	cancel()
	time.Sleep(100 * time.Millisecond)
	removes := mockDB.getRemoveCalls()
	assert.Assert(t, len(removes) >= 8 && len(removes) <= 11, "expected to have 8-11 remove calls, got %d", len(removes))
}
