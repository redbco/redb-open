package logger

import (
	"fmt"
	"log"
	"os"
)

type Logger struct {
	serviceName string
	version     string
	logger      *log.Logger
}

func New(serviceName, version string) *Logger {
	return &Logger{
		serviceName: serviceName,
		version:     version,
		logger:      log.New(os.Stdout, fmt.Sprintf("[%s] ", serviceName), log.LstdFlags),
	}
}

func (l *Logger) Debug(message string) {
	l.logger.Printf("DEBUG: %s", message)
}

func (l *Logger) Info(message string) {
	l.logger.Printf("INFO: %s", message)
}

func (l *Logger) Infof(format string, args ...interface{}) {
	l.logger.Printf("INFO: "+format, args...)
}

func (l *Logger) Warn(message string) {
	l.logger.Printf("WARN: %s", message)
}

func (l *Logger) Warnf(format string, args ...interface{}) {
	l.logger.Printf("WARN: "+format, args...)
}

func (l *Logger) Error(message string) {
	l.logger.Printf("ERROR: %s", message)
}

func (l *Logger) Errorf(format string, args ...interface{}) {
	l.logger.Printf("ERROR: "+format, args...)
}

func (l *Logger) Fatal(message string) {
	l.logger.Fatalf("FATAL: %s", message)
}

func (l *Logger) Fatalf(format string, args ...interface{}) {
	l.logger.Fatalf("FATAL: "+format, args...)
}
