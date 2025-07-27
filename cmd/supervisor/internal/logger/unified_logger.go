package logger

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	commonv1 "github.com/redbco/redb-open/api/proto/common/v1"
	"github.com/redbco/redb-open/pkg/syslog"
)

// ANSI color codes for console output
const (
	ColorReset  = "\033[0m"
	ColorRed    = "\033[31m"
	ColorYellow = "\033[33m"
	ColorBlue   = "\033[34m"
	ColorGray   = "\033[37m"
	ColorGreen  = "\033[32m"
	ColorCyan   = "\033[36m"

	// Bright colors
	ColorBrightRed    = "\033[91m"
	ColorBrightYellow = "\033[93m"
	ColorBrightBlue   = "\033[94m"
	ColorBrightGray   = "\033[90m"
)

// Column widths for better alignment
const (
	ServiceNameWidth = 20 // Fixed width for service names
	LogLevelWidth    = 7  // Fixed width for log levels (ERROR, WARN, etc.) - icons add +2
)

// UnifiedLogger handles both supervisor's own logs and logs from microservices
// It outputs all logs to console and writes them to the same log file
type UnifiedLogger struct {
	serviceName string
	version     string

	mu            sync.Mutex
	fileWriter    io.Writer
	consoleLogger *log.Logger
	logLevel      syslog.LogLevel
	initialized   bool
	colorEnabled  bool
}

// NewUnifiedLogger creates a unified logger that handles both supervisor and microservice logs
func NewUnifiedLogger(serviceName, version string, logFile string, logLevel string) *UnifiedLogger {
	logger := &UnifiedLogger{
		serviceName:   serviceName,
		version:       version,
		consoleLogger: log.New(os.Stdout, "", 0), // No prefix, we'll format ourselves
		colorEnabled:  isTerminal(),              // Enable colors if outputting to terminal
	}

	// Parse log level
	switch logLevel {
	case "debug":
		logger.logLevel = syslog.DEBUG
	case "info":
		logger.logLevel = syslog.INFO
	case "warn":
		logger.logLevel = syslog.WARN
	case "error":
		logger.logLevel = syslog.ERROR
	default:
		logger.logLevel = syslog.INFO
	}

	// Setup file writer
	if logFile != "" {
		// Ensure the log directory exists
		if err := os.MkdirAll(filepath.Dir(logFile), 0755); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to create log directory: %v\n", err)
			logger.initialized = false
			return logger
		}

		// Open the log file in append mode
		file, err := os.OpenFile(logFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to open log file: %v\n", err)
			logger.initialized = false
			return logger
		}
		logger.fileWriter = file
		logger.initialized = true
	}

	return logger
}

// isTerminal checks if we're outputting to a terminal (for color support)
func isTerminal() bool {
	if os.Getenv("TERM") == "dumb" {
		return false
	}
	fileInfo, _ := os.Stdout.Stat()
	return (fileInfo.Mode() & os.ModeCharDevice) != 0
}

// getColorForLevel returns the appropriate color for a log level
func (l *UnifiedLogger) getColorForLevel(level syslog.LogLevel) string {
	if !l.colorEnabled {
		return ""
	}

	switch level {
	case syslog.DEBUG:
		return ColorBrightGray
	case syslog.INFO:
		return ColorGreen
	case syslog.WARN:
		return ColorBrightYellow
	case syslog.ERROR:
		return ColorBrightRed
	default:
		return ColorReset
	}
}

// formatServiceName truncates and pads service name for consistent column width
func formatServiceName(serviceName string) string {
	if len(serviceName) > ServiceNameWidth {
		// Truncate long service names but keep some of the instance ID if present
		if idx := len(serviceName) - 10; idx > 0 && serviceName[idx-1] == '[' {
			// Try to keep the instance ID part: "verylongservice[12345678]" -> "verylon[12345678]"
			prefix := serviceName[:ServiceNameWidth-10]
			suffix := serviceName[idx-1:]
			return prefix + suffix
		} else {
			// Simple truncation
			return serviceName[:ServiceNameWidth-1] + "…"
		}
	}
	// Pad short names
	return fmt.Sprintf("%-*s", ServiceNameWidth, serviceName)
}

// formatLogLevel pads log level for consistent column width and adds visual indicators
func formatLogLevel(level syslog.LogLevel) string {
	levelStr := level.String()

	// Add visual indicators for different levels
	switch level {
	case syslog.ERROR:
		levelStr = "✗ " + levelStr
	case syslog.WARN:
		levelStr = "⚠ " + levelStr
	case syslog.INFO:
		levelStr = "ℹ " + levelStr
	case syslog.DEBUG:
		levelStr = "◦ " + levelStr
	}

	return fmt.Sprintf("%-*s", LogLevelWidth+2, levelStr) // +2 for the icon
}

// logToConsoleAndFile logs a message to both console and file
func (l *UnifiedLogger) logToConsoleAndFile(service string, level syslog.LogLevel, message string) {
	if level < l.logLevel {
		return
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	timestamp := time.Now().Format("2006-01-02 15:04:05.000")

	// Format for file (no colors, consistent with current format)
	fileLogLine := fmt.Sprintf("[%s] [%s] [%s] %s", timestamp, service, level, message)

	// Format for console (with colors and column alignment)
	color := l.getColorForLevel(level)
	resetColor := ""
	if l.colorEnabled {
		resetColor = ColorReset
	}

	formattedService := formatServiceName(service)
	formattedLevel := formatLogLevel(level)

	consoleLogLine := fmt.Sprintf("%s[%s] [%s] [%s%s%s] %s%s",
		ColorCyan, timestamp, formattedService, color, formattedLevel, resetColor, message, resetColor)

	// Always output formatted version to console
	l.consoleLogger.Println(consoleLogLine)

	// Write plain version to file if available
	if l.initialized && l.fileWriter != nil {
		if _, err := l.fileWriter.Write([]byte(fileLogLine + "\n")); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to write to log file: %v\n", err)
		}
	}
}

// LogMicroserviceEntry logs an entry from a microservice to both console and file
func (l *UnifiedLogger) LogMicroserviceEntry(entry *commonv1.LogEntry) {
	if entry == nil || entry.Service == nil {
		return
	}

	// Convert protobuf log level to syslog level
	var level syslog.LogLevel
	switch entry.Level {
	case commonv1.LogLevel_LOG_LEVEL_DEBUG:
		level = syslog.DEBUG
	case commonv1.LogLevel_LOG_LEVEL_INFO:
		level = syslog.INFO
	case commonv1.LogLevel_LOG_LEVEL_WARN:
		level = syslog.WARN
	case commonv1.LogLevel_LOG_LEVEL_ERROR:
		level = syslog.ERROR
	default:
		level = syslog.INFO
	}

	// Check log level filter
	if level < l.logLevel {
		return
	}

	serviceName := entry.Service.Name
	if entry.Service.InstanceId != "" {
		serviceName = fmt.Sprintf("%s[%s]", entry.Service.Name, entry.Service.InstanceId[:8])
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	// Convert the timestamp to local timezone for consistent display
	timestamp := entry.Timestamp.AsTime().Local().Format("2006-01-02 15:04:05.000")

	// Format for file (no colors, consistent with current format)
	fileLogLine := fmt.Sprintf("[%s] [%s] [%s] %s", timestamp, serviceName, level, entry.Message)

	// Format for console (with colors and column alignment)
	color := l.getColorForLevel(level)
	resetColor := ""
	if l.colorEnabled {
		resetColor = ColorReset
	}

	formattedService := formatServiceName(serviceName)
	formattedLevel := formatLogLevel(level)

	consoleLogLine := fmt.Sprintf("%s[%s] [%s] [%s%s%s] %s%s",
		ColorCyan, timestamp, formattedService, color, formattedLevel, resetColor, entry.Message, resetColor)

	// Always output formatted version to console
	l.consoleLogger.Println(consoleLogLine)

	// Write plain version to file if available
	if l.initialized && l.fileWriter != nil {
		if _, err := l.fileWriter.Write([]byte(fileLogLine + "\n")); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to write to log file: %v\n", err)
		}
	}
}

// LoggerInterface implementation for supervisor's own logs
func (l *UnifiedLogger) Debug(message string) {
	l.logToConsoleAndFile(l.serviceName, syslog.DEBUG, message)
}

func (l *UnifiedLogger) Info(message string) {
	l.logToConsoleAndFile(l.serviceName, syslog.INFO, message)
}

func (l *UnifiedLogger) Infof(format string, args ...interface{}) {
	message := fmt.Sprintf(format, args...)
	l.Info(message)
}

func (l *UnifiedLogger) Warn(message string) {
	l.logToConsoleAndFile(l.serviceName, syslog.WARN, message)
}

func (l *UnifiedLogger) Warnf(format string, args ...interface{}) {
	message := fmt.Sprintf(format, args...)
	l.Warn(message)
}

func (l *UnifiedLogger) Error(message string) {
	l.logToConsoleAndFile(l.serviceName, syslog.ERROR, message)
}

func (l *UnifiedLogger) Errorf(format string, args ...interface{}) {
	message := fmt.Sprintf(format, args...)
	l.Error(message)
}

func (l *UnifiedLogger) Fatal(message string) {
	l.Error(message)
	os.Exit(1)
}

func (l *UnifiedLogger) Fatalf(format string, args ...interface{}) {
	message := fmt.Sprintf(format, args...)
	l.Fatal(message)
}

// Close closes the file writer if it exists
func (l *UnifiedLogger) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if closer, ok := l.fileWriter.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}
