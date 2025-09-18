use std::fmt;
use tracing::{Event, Subscriber};
use tracing_subscriber::fmt::{format::Writer, FmtContext, FormatEvent, FormatFields};
use tracing_subscriber::registry::LookupSpan;

/// ANSI color codes for console output (matching Golang logger)
const COLOR_RESET: &str = "\x1b[0m";
const COLOR_CYAN: &str = "\x1b[36m";
const COLOR_GREEN: &str = "\x1b[32m";
const COLOR_BRIGHT_YELLOW: &str = "\x1b[93m";
const COLOR_BRIGHT_RED: &str = "\x1b[91m";
const COLOR_BRIGHT_GRAY: &str = "\x1b[90m";

/// Column widths for better alignment (same as Golang services)
const SERVICE_NAME_WIDTH: usize = 20;
const LOG_LEVEL_WIDTH: usize = 7; // +2 for icons

/// Custom formatter that matches the Golang service log format
pub struct RedbLogFormatter {
    service_name: String,
    color_enabled: bool,
}

/// Macro to create component-specific logging functions
#[macro_export]
macro_rules! component_info {
    ($component:expr, $($arg:tt)*) => {
        tracing::info!(component = $component, $($arg)*)
    };
}

#[macro_export]
macro_rules! component_warn {
    ($component:expr, $($arg:tt)*) => {
        tracing::warn!(component = $component, $($arg)*)
    };
}

#[macro_export]
macro_rules! component_debug {
    ($component:expr, $($arg:tt)*) => {
        tracing::debug!(component = $component, $($arg)*)
    };
}

#[macro_export]
macro_rules! component_error {
    ($component:expr, $($arg:tt)*) => {
        tracing::error!(component = $component, $($arg)*)
    };
}

impl RedbLogFormatter {
    pub fn new(service_name: String) -> Self {
        let color_enabled = is_terminal();
        Self {
            service_name,
            color_enabled,
        }
    }

    /// Format service name with fixed width (matching Golang implementation)
    fn format_service_name(&self, component: Option<&str>) -> String {
        let name = if let Some(comp) = component {
            format!("mesh-{}", comp)
        } else {
            self.service_name.clone()
        };
        
        if name.len() > SERVICE_NAME_WIDTH {
            // Truncate long service names
            format!("{}…", &name[..SERVICE_NAME_WIDTH - 1])
        } else {
            // Pad short names
            format!("{:<width$}", name, width = SERVICE_NAME_WIDTH)
        }
    }

    /// Format log level with visual indicators (matching Golang implementation)
    fn format_log_level(&self, level: &tracing::Level) -> String {
        let level_str = match *level {
            tracing::Level::ERROR => "✗ ERROR",
            tracing::Level::WARN => "⚠ WARN",
            tracing::Level::INFO => "ℹ INFO",
            tracing::Level::DEBUG => "◦ DEBUG",
            tracing::Level::TRACE => "◦ TRACE",
        };
        
        format!("{:<width$}", level_str, width = LOG_LEVEL_WIDTH + 2) // +2 for icon
    }

    /// Get color for log level (matching Golang implementation)
    fn get_color_for_level(&self, level: &tracing::Level) -> &'static str {
        if !self.color_enabled {
            return "";
        }

        match *level {
            tracing::Level::ERROR => COLOR_BRIGHT_RED,
            tracing::Level::WARN => COLOR_BRIGHT_YELLOW,
            tracing::Level::INFO => COLOR_GREEN,
            tracing::Level::DEBUG => COLOR_BRIGHT_GRAY,
            tracing::Level::TRACE => COLOR_BRIGHT_GRAY,
        }
    }
}

impl<S, N> FormatEvent<S, N> for RedbLogFormatter
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        _ctx: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> fmt::Result {
        // Get current timestamp in the same format as Golang services
        let now = chrono::Local::now();
        let timestamp = now.format("%Y-%m-%d %H:%M:%S%.3f").to_string();

        // Get log level
        let level = event.metadata().level();
        
        // Extract component field if present
        let mut visitor = FieldVisitor::new();
        event.record(&mut visitor);
        
        // Format components
        let formatted_service = self.format_service_name(visitor.component.as_deref());
        let formatted_level = self.format_log_level(level);
        
        // Get colors
        let color = self.get_color_for_level(level);
        let reset_color = if self.color_enabled { COLOR_RESET } else { "" };
        let cyan_color = if self.color_enabled { COLOR_CYAN } else { "" };

        // Write the formatted log line matching Golang format:
        // [timestamp] [service_name] [log_level] message
        write!(
            writer,
            "{}[{}] [{}] [{}{}{}] ",
            cyan_color, timestamp, formatted_service, color, formatted_level, reset_color
        )?;

        // Write the message (already extracted by FieldVisitor)
        writeln!(writer, "{}{}", visitor.message, reset_color)?;

        Ok(())
    }
}

/// Visitor to extract fields from the event
struct FieldVisitor {
    message: String,
    component: Option<String>,
}

impl FieldVisitor {
    fn new() -> Self {
        Self {
            message: String::new(),
            component: None,
        }
    }
}

impl tracing::field::Visit for FieldVisitor {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn fmt::Debug) {
        match field.name() {
            "message" => {
                self.message = format!("{:?}", value);
                // Remove quotes from debug formatting
                if self.message.starts_with('"') && self.message.ends_with('"') {
                    self.message = self.message[1..self.message.len()-1].to_string();
                }
            }
            "component" => {
                let comp_str = format!("{:?}", value);
                if comp_str.starts_with('"') && comp_str.ends_with('"') {
                    self.component = Some(comp_str[1..comp_str.len()-1].to_string());
                } else {
                    self.component = Some(comp_str);
                }
            }
            _ => {}
        }
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        match field.name() {
            "message" => {
                self.message = value.to_string();
            }
            "component" => {
                self.component = Some(value.to_string());
            }
            _ => {}
        }
    }
}

/// Check if we're outputting to a terminal (for color support)
fn is_terminal() -> bool {
    if std::env::var("TERM").unwrap_or_default() == "dumb" {
        return false;
    }
    
    // Simple check - in a real implementation you might use a crate like `atty`
    std::env::var("TERM").is_ok()
}
