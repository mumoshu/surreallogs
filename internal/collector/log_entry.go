package collector

import "time"

// LogEntry is a log entry as it is observed by the collector
// and written to the buffer
type LogEntry struct {
	// Timestamp is the time that the collector observed the log entry as it was written
	Timestamp time.Time `json:"timestamp"`
	// Message is the log message
	Message map[string]interface{} `json:"message"`
	// Source is the name of the instance that wrote the log entry to the file
	Source string `json:"source"`
	// File is the name of the log file containing the log entry
	File string `json:"file"`
}
