package log

import (
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type logOutput struct {
	started time.Time
	message string
	fields  []zapcore.Field
	level   zapcore.Level
	count   uint64
}

type MessageCollapsingLogger struct {
	logEntry map[string]*logOutput
	stop     chan struct{}
	logger   *zap.Logger

	sync.RWMutex
}

func NewMessageCollapsingLogger(handle *LoggerHandle) *MessageCollapsingLogger {
	cl := &MessageCollapsingLogger{
		logEntry: make(map[string]*logOutput),
		stop:     make(chan struct{}),
		logger:   Log(handle),
	}

	go func() {
		for {
			select {
			case <-cl.stop:
				return
			case <-time.After(time.Second):
				cl.collapseMessages()
			}
		}
	}()

	return cl
}

func (cl *MessageCollapsingLogger) collapseMessages() {
	cl.Lock()
	defer cl.Unlock()
	for key, output := range cl.logEntry {
		if time.Since(output.started) >= time.Second && output.count > 1 {
			msg := fmt.Sprintf("The following output was logged %d time(s) in the last %d second(s): %s", output.count, 1, output.message)
			cl.zapLog(msg, output.level, output.fields)
			delete(cl.logEntry, key)
		}
	}
}

func (cl *MessageCollapsingLogger) Stop() {
	close(cl.stop)
}

func (cl *MessageCollapsingLogger) Info(message string, fields ...zap.Field) {
	cl.processLog(message, zapcore.InfoLevel, fields)
}

func (cl *MessageCollapsingLogger) Debug(message string, fields ...zap.Field) {
	cl.processLog(message, zapcore.DebugLevel, fields)
}

func (cl *MessageCollapsingLogger) DPanic(message string, fields ...zap.Field) {
	cl.processLog(message, zapcore.DPanicLevel, fields)
}

func (cl *MessageCollapsingLogger) Fatal(message string, fields ...zap.Field) {
	cl.processLog(message, zapcore.FatalLevel, fields)
}

func (cl *MessageCollapsingLogger) Panic(message string, fields ...zap.Field) {
	cl.processLog(message, zapcore.PanicLevel, fields)
}

func (cl *MessageCollapsingLogger) Warn(message string, fields ...zap.Field) {
	cl.processLog(message, zapcore.WarnLevel, fields)
}

func (cl *MessageCollapsingLogger) processLog(message string, level zapcore.Level, fields []zapcore.Field) {
	cl.Lock()
	defer cl.Unlock()

	if output, ok := cl.logEntry[message]; ok {
		if time.Since(output.started) >= time.Second {
			msg := message
			if output.count > 1 {
				msg = fmt.Sprintf("The following output was logged %d time(s) in the last %d second(s): %s", output.count, 1, output.message)
			}
			cl.zapLog(msg, level, fields)
			delete(cl.logEntry, message)
		} else {
			output.count++
		}
	} else {
		cl.logEntry[message] = &logOutput{
			fields:  fields,
			message: message,
			count:   1,
			level:   level,
			started: time.Now(),
		}
		cl.zapLog(message, level, fields)
	}
}

func (cl *MessageCollapsingLogger) zapLog(message string, level zapcore.Level, fields []zapcore.Field) {
	if ce := cl.logger.Check(level, message); ce != nil {
		ce.Write(fields...)
	}
}
