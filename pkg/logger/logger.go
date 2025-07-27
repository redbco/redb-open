package logger

import (
	"fmt"
	"os"
	"sync"
	"time"

	commonv1 "github.com/redbco/redb-open/api/proto/common/v1"
)

// ANSI color codes for console output
const (
	ColorReset        = "\033[0m"
	ColorRed          = "\033[31m"
	ColorYellow       = "\033[33m"
	ColorBlue         = "\033[34m"
	ColorGray         = "\033[37m"
	ColorGreen        = "\033[32m"
	ColorCyan         = "\033[36m"
	ColorBrightRed    = "\033[91m"
	ColorBrightYellow = "\033[93m"
	ColorBrightBlue   = "\033[94m"
	ColorBrightGray   = "\033[90m"
)

// Column widths for better alignment (same as supervisor)
const (
	ServiceNameWidth = 20 // Fixed width for service names
	LogLevelWidth    = 7  // Fixed width for log levels (ERROR, WARN, etc.) - icons add +2
)

// LogEntry represents a single log entry
type LogEntry struct {
	Time    time.Time
	Level   string
	Message string
	Fields  map[string]string
	TraceID string
}

// Logger provides structured logging with streaming support
type Logger struct {
	serviceName string
	version     string

	mu             sync.RWMutex
	subscribers    []chan LogEntry
	colorEnabled   bool
	disableConsole bool // New flag to disable console output when streaming to supervisor
}

// New creates a new logger instance
func New(serviceName, version string) *Logger {
	return &Logger{
		serviceName:    serviceName,
		version:        version,
		subscribers:    make([]chan LogEntry, 0),
		colorEnabled:   isTerminal(),
		disableConsole: false,
	}
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
func (l *Logger) getColorForLevel(level string) string {
	if !l.colorEnabled {
		return ""
	}

	switch level {
	case "DEBUG":
		return ColorBrightGray
	case "INFO":
		return ColorGreen
	case "WARN":
		return ColorBrightYellow
	case "ERROR":
		return ColorBrightRed
	case "FATAL":
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
func formatLogLevel(level string) string {
	levelStr := level

	// Add visual indicators for different levels
	switch level {
	case "ERROR", "FATAL":
		levelStr = "✗ " + levelStr
	case "WARN":
		levelStr = "⚠ " + levelStr
	case "INFO":
		levelStr = "ℹ " + levelStr
	case "DEBUG":
		levelStr = "◦ " + levelStr
	}

	return fmt.Sprintf("%-*s", LogLevelWidth+2, levelStr) // +2 for the icon
}

// Subscribe returns a channel to receive log entries
func (l *Logger) Subscribe() <-chan LogEntry {
	ch := make(chan LogEntry, 100)

	l.mu.Lock()
	l.subscribers = append(l.subscribers, ch)
	l.mu.Unlock()

	return ch
}

// DisableConsoleOutput disables console output when streaming to supervisor
func (l *Logger) DisableConsoleOutput() {
	l.mu.Lock()
	l.disableConsole = true
	l.mu.Unlock()
}

// EnableConsoleOutput enables console output (default behavior)
func (l *Logger) EnableConsoleOutput() {
	l.mu.Lock()
	l.disableConsole = false
	l.mu.Unlock()
}

func (l *Logger) log(level, message string, fields map[string]string) {
	now := time.Now()
	entry := LogEntry{
		Time:    now,
		Level:   level,
		Message: message,
		Fields:  fields,
	}

	// Check if we should output to console
	l.mu.RLock()
	shouldOutputToConsole := !l.disableConsole
	l.mu.RUnlock()

	// Output to console only if not disabled
	if shouldOutputToConsole {
		// Format console output with enhanced formatting (same as supervisor)
		timestamp := now.Format("2006-01-02 15:04:05.000")

		color := l.getColorForLevel(level)
		resetColor := ""
		if l.colorEnabled {
			resetColor = ColorReset
		}

		formattedService := formatServiceName(l.serviceName)
		formattedLevel := formatLogLevel(level)

		consoleLogLine := fmt.Sprintf("%s[%s] [%s] [%s%s%s] %s%s",
			ColorCyan, timestamp, formattedService, color, formattedLevel, resetColor, message, resetColor)

		// Output enhanced format to console
		fmt.Println(consoleLogLine)
	}

	// Always send to subscribers (supervisor) if any
	l.mu.RLock()
	for _, ch := range l.subscribers {
		select {
		case ch <- entry:
		default:
			// Skip if channel is full
		}
	}
	l.mu.RUnlock()
}

// Debug logs a debug message with optional formatting
func (l *Logger) Debug(message string, args ...interface{}) {
	if len(args) > 0 {
		l.log("DEBUG", fmt.Sprintf(message, args...), nil)
	} else {
		l.log("DEBUG", message, nil)
	}
}

// Debugf logs a formatted debug message
func (l *Logger) Debugf(format string, args ...interface{}) {
	l.log("DEBUG", fmt.Sprintf(format, args...), nil)
}

// Info logs an info message with optional formatting
func (l *Logger) Info(message string, args ...interface{}) {
	if len(args) > 0 {
		l.log("INFO", fmt.Sprintf(message, args...), nil)
	} else {
		l.log("INFO", message, nil)
	}
}

// Infof logs a formatted info message
func (l *Logger) Infof(format string, args ...interface{}) {
	l.log("INFO", fmt.Sprintf(format, args...), nil)
}

// Warn logs a warning message with optional formatting
func (l *Logger) Warn(message string, args ...interface{}) {
	if len(args) > 0 {
		l.log("WARN", fmt.Sprintf(message, args...), nil)
	} else {
		l.log("WARN", message, nil)
	}
}

// Warnf logs a formatted warning message
func (l *Logger) Warnf(format string, args ...interface{}) {
	l.log("WARN", fmt.Sprintf(format, args...), nil)
}

// Error logs an error message with optional formatting
func (l *Logger) Error(message string, args ...interface{}) {
	if len(args) > 0 {
		l.log("ERROR", fmt.Sprintf(message, args...), nil)
	} else {
		l.log("ERROR", message, nil)
	}
}

// Errorf logs a formatted error message
func (l *Logger) Errorf(format string, args ...interface{}) {
	l.log("ERROR", fmt.Sprintf(format, args...), nil)
}

// Fatal logs a fatal message and exits
func (l *Logger) Fatal(message string) {
	l.log("FATAL", message, nil)
	os.Exit(1)
}

// Fatalf logs a formatted fatal message and exits
func (l *Logger) Fatalf(format string, args ...interface{}) {
	message := fmt.Sprintf(format, args...)
	l.log("FATAL", message, nil)
	os.Exit(1)
}

// WithFields logs a message with additional fields
func (l *Logger) WithFields(fields map[string]string) *LogContext {
	return &LogContext{
		logger: l,
		fields: fields,
	}
}

// Rotate performs log rotation
func (l *Logger) Rotate() {
	// Implement log rotation logic if needed
	l.Info("Log rotation requested")
}

// LogContext provides field-based logging
type LogContext struct {
	logger *Logger
	fields map[string]string
}

func (c *LogContext) Info(message string) {
	c.logger.log("INFO", message, c.fields)
}

func (c *LogContext) Error(message string) {
	c.logger.log("ERROR", message, c.fields)
}

// MapLogLevel maps string log level to proto enum
func MapLogLevel(level string) commonv1.LogLevel {
	switch level {
	case "DEBUG":
		return commonv1.LogLevel_LOG_LEVEL_DEBUG
	case "INFO":
		return commonv1.LogLevel_LOG_LEVEL_INFO
	case "WARN":
		return commonv1.LogLevel_LOG_LEVEL_WARN
	case "ERROR":
		return commonv1.LogLevel_LOG_LEVEL_ERROR
	case "FATAL":
		return commonv1.LogLevel_LOG_LEVEL_FATAL
	default:
		return commonv1.LogLevel_LOG_LEVEL_UNSPECIFIED
	}
}
