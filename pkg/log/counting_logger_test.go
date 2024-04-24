package log

import (
	"fmt"
	"testing"
	"time"

	"go.uber.org/zap"
)

func TestCollapsingLogEntries(t *testing.T) {
	cl := NewMessageCollapsingLogger(SchedNode)
	cl.Info("test log message", zap.String("key", "val"))
	cl.Info("test log message", zap.String("key", "val"))
	cl.Info("test log message", zap.String("key", "val"))
	cl.Info("another test", zap.String("key", "val"))
	cl.Info("another test", zap.String("key", "val"))
	time.Sleep(time.Millisecond * 500)
	fmt.Println("Slept 500ms")
	cl.Info("test log message", zap.String("key", "val"))
	time.Sleep(time.Second)
	fmt.Println("Slept 1 s")
	cl.Info("test log message", zap.String("key", "val"))
	cl.Info("another test", zap.String("key", "val"))
}
