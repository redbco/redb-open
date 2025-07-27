package logger

import commonv1 "github.com/redbco/redb-open/api/proto/common/v1"

// LoggerInterface defines the interface that all loggers must implement
type LoggerInterface interface {
	Debug(message string)
	Info(message string)
	Infof(format string, args ...interface{})
	Warn(message string)
	Warnf(format string, args ...interface{})
	Error(message string)
	Errorf(format string, args ...interface{})
	Fatal(message string)
	Fatalf(format string, args ...interface{})
}

// UnifiedLoggerInterface extends LoggerInterface with microservice log handling
type UnifiedLoggerInterface interface {
	LoggerInterface
	LogMicroserviceEntry(entry *commonv1.LogEntry)
	Close() error
}
