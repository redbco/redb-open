package logger

import (
	"fmt"
	"os"

	"github.com/redbco/redb-open/pkg/syslog"
)

// SyslogLogger wraps the syslog package to provide the same interface as the current Logger
type SyslogLogger struct {
	serviceName string
	version     string
	initialized bool
}

// NewSyslogLogger creates a new logger that uses the syslog package
func NewSyslogLogger(serviceName, version string, logFile string, logLevel string) *SyslogLogger {
	logger := &SyslogLogger{
		serviceName: serviceName,
		version:     version,
	}

	// Initialize syslog
	syslogConfig := syslog.Config{
		LogFile:  logFile,
		LogLevel: syslog.INFO, // Default
	}

	// Parse log level
	switch logLevel {
	case "debug":
		syslogConfig.LogLevel = syslog.DEBUG
	case "info":
		syslogConfig.LogLevel = syslog.INFO
	case "warn":
		syslogConfig.LogLevel = syslog.WARN
	case "error":
		syslogConfig.LogLevel = syslog.ERROR
	}

	if err := syslog.Init(syslogConfig); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize syslog: %v\n", err)
		// Fall back to stdout logging
		logger.initialized = false
	} else {
		logger.initialized = true
	}

	return logger
}

func (l *SyslogLogger) Debug(message string) {
	if l.initialized {
		syslog.Debug(l.serviceName, message)
	} else {
		fmt.Printf("DEBUG [%s]: %s\n", l.serviceName, message)
	}
}

func (l *SyslogLogger) Info(message string) {
	if l.initialized {
		syslog.Info(l.serviceName, message)
	} else {
		fmt.Printf("INFO [%s]: %s\n", l.serviceName, message)
	}
}

func (l *SyslogLogger) Infof(format string, args ...interface{}) {
	message := fmt.Sprintf(format, args...)
	l.Info(message)
}

func (l *SyslogLogger) Warn(message string) {
	if l.initialized {
		syslog.Warn(l.serviceName, message)
	} else {
		fmt.Printf("WARN [%s]: %s\n", l.serviceName, message)
	}
}

func (l *SyslogLogger) Warnf(format string, args ...interface{}) {
	message := fmt.Sprintf(format, args...)
	l.Warn(message)
}

func (l *SyslogLogger) Error(message string) {
	if l.initialized {
		syslog.Error(l.serviceName, message)
	} else {
		fmt.Printf("ERROR [%s]: %s\n", l.serviceName, message)
	}
}

func (l *SyslogLogger) Errorf(format string, args ...interface{}) {
	message := fmt.Sprintf(format, args...)
	l.Error(message)
}

func (l *SyslogLogger) Fatal(message string) {
	l.Error(message)
	os.Exit(1)
}

func (l *SyslogLogger) Fatalf(format string, args ...interface{}) {
	message := fmt.Sprintf(format, args...)
	l.Fatal(message)
}
