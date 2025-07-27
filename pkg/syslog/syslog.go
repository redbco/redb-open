package syslog

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

var (
	// Default logger instance
	defaultLogger *Logger
	once          sync.Once
)

// LogLevel represents the severity level of a log message
type LogLevel int

const (
	DEBUG LogLevel = iota
	INFO
	WARN
	ERROR
)

// String returns the string representation of the log level
func (l LogLevel) String() string {
	switch l {
	case DEBUG:
		return "DEBUG"
	case INFO:
		return "INFO"
	case WARN:
		return "WARN"
	case ERROR:
		return "ERROR"
	default:
		return "UNKNOWN"
	}
}

// Logger represents a system logger instance
type Logger struct {
	mu       sync.Mutex
	writer   io.Writer
	logLevel LogLevel
}

// Config holds the configuration for the logger
type Config struct {
	LogFile  string
	LogLevel LogLevel
}

// Init initializes the default logger with the given configuration
func Init(config Config) error {
	var initErr error
	once.Do(func() {
		defaultLogger, initErr = newLogger(config)
	})
	return initErr
}

// newLogger creates a new logger instance
func newLogger(config Config) (*Logger, error) {
	var writer io.Writer = os.Stdout

	if config.LogFile != "" {
		// Ensure the log directory exists
		if err := os.MkdirAll(filepath.Dir(config.LogFile), 0755); err != nil {
			return nil, fmt.Errorf("failed to create log directory: %v", err)
		}

		// Open the log file in append mode
		file, err := os.OpenFile(config.LogFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return nil, fmt.Errorf("failed to open log file: %v", err)
		}
		writer = file
	}

	return &Logger{
		writer:   writer,
		logLevel: config.LogLevel,
	}, nil
}

// log writes a log message with the given level and format
func (l *Logger) log(service string, level LogLevel, format string, args ...interface{}) {
	if level < l.logLevel {
		return
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	timestamp := time.Now().Format("2006-01-02 15:04:05.000")
	message := fmt.Sprintf(format, args...)
	logLine := fmt.Sprintf("[%s] [%s] [%s] %s\n", timestamp, service, level, message)

	if _, err := l.writer.Write([]byte(logLine)); err != nil {
		log.Printf("Failed to write log: %v", err)
	}
}

// Debug logs a debug message
func (l *Logger) Debug(service string, format string, args ...interface{}) {
	l.log(service, DEBUG, format, args...)
}

// Info logs an info message
func (l *Logger) Info(service string, format string, args ...interface{}) {
	l.log(service, INFO, format, args...)
}

// Warn logs a warning message
func (l *Logger) Warn(service string, format string, args ...interface{}) {
	l.log(service, WARN, format, args...)
}

// Error logs an error message
func (l *Logger) Error(service string, format string, args ...interface{}) {
	l.log(service, ERROR, format, args...)
}

// Close closes the logger and its underlying resources
func (l *Logger) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if closer, ok := l.writer.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

// Package-level logging functions that use the default logger

// Debug logs a debug message using the default logger
func Debug(service string, format string, args ...interface{}) {
	if defaultLogger != nil {
		defaultLogger.Debug(service, format, args...)
	}
}

// Info logs an info message using the default logger
func Info(service string, format string, args ...interface{}) {
	if defaultLogger != nil {
		defaultLogger.Info(service, format, args...)
	}
}

// Warn logs a warning message using the default logger
func Warn(service string, format string, args ...interface{}) {
	if defaultLogger != nil {
		defaultLogger.Warn(service, format, args...)
	}
}

// Error logs an error message using the default logger
func Error(service string, format string, args ...interface{}) {
	if defaultLogger != nil {
		defaultLogger.Error(service, format, args...)
	}
}

// Close closes the default logger
func Close() error {
	if defaultLogger != nil {
		return defaultLogger.Close()
	}
	return nil
}
