package logger

import (
	"context"
	"log"
)

// Logger defines the logging interface
type Logger interface {
	Info(ctx context.Context, msg string, keysAndValues ...interface{})
	Error(ctx context.Context, msg string, keysAndValues ...interface{})
	Debug(ctx context.Context, msg string, keysAndValues ...interface{})
}

// DefaultLogger implements Logger using the standard log package
type DefaultLogger struct {
	logger *log.Logger
}

// NewDefaultLogger creates a new default logger
func NewDefaultLogger() *DefaultLogger {
	return &DefaultLogger{
		logger: log.New(log.Writer(), "[clientapi] ", log.LstdFlags),
	}
}

func (l *DefaultLogger) Info(ctx context.Context, msg string, keysAndValues ...interface{}) {
	l.logger.Printf("[INFO] %s %v", msg, keysAndValues)
}

func (l *DefaultLogger) Error(ctx context.Context, msg string, keysAndValues ...interface{}) {
	l.logger.Printf("[ERROR] %s %v", msg, keysAndValues)
}

func (l *DefaultLogger) Debug(ctx context.Context, msg string, keysAndValues ...interface{}) {
	l.logger.Printf("[DEBUG] %s %v", msg, keysAndValues)
}
