package log

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log/slog"
	"regexp"
	"strings"
	"time"

	logs "github.com/hades-scheduler/hades/shared/buildlogs"
)

const (
	// StreamStdout identifies stdout log stream
	StreamStdout = "stdout"
	// StreamStderr identifies stderr log stream
	StreamStderr = "stderr"
)

var (
	// logLineRegex matches structured logs with format: time="..." level="..." msg="..."
	logLineRegex = regexp.MustCompile(`time="[^"]*".*level="[^"]*".*msg="[^"]*"`)
)

// LogParser defines the interface for parsing container logs
type LogParser interface {
	ParseContainerLogs(containerID string, jobID string) (logs.Log, error)
}

// StdLogParser parses Docker container logs from stdout and stderr streams
type StdLogParser struct {
	stdout *bytes.Buffer
	stderr *bytes.Buffer
}

// NewStdLogParser creates a new LogParser from stdout and stderr buffers.
// Both stdout and stderr must be non-nil.
func NewStdLogParser(stdout, stderr *bytes.Buffer) LogParser {
	if stdout == nil {
		stdout = &bytes.Buffer{}
	}
	if stderr == nil {
		stderr = &bytes.Buffer{}
	}

	return &StdLogParser{
		stdout: stdout,
		stderr: stderr,
	}
}

// ParseContainerLogs converts raw standard log streams into structured log entries.
// It processes both stdout and stderr, preserving timestamps and stream types.
func (p *StdLogParser) ParseContainerLogs(containerID string, jobID string) (logs.Log, error) {
	buildJobLog := logs.Log{
		JobID:       jobID,
		ContainerID: containerID,
		Logs:        make([]logs.LogEntry, 0),
	}

	streams := []struct {
		buf        *bytes.Buffer
		streamType string
	}{
		{buf: p.stdout, streamType: StreamStdout},
		{buf: p.stderr, streamType: StreamStderr},
	}

	for _, stream := range streams {
		if err := processStream(stream.buf, stream.streamType, &buildJobLog.Logs); err != nil {
			return buildJobLog, fmt.Errorf("processing %s: %w", stream.streamType, err)
		}
	}

	slog.Debug("Parsed container logs",
		"container_id", containerID,
		"job_id", jobID,
		"entries_count", len(buildJobLog.Logs))

	return buildJobLog, nil
}

// processStream handles a single log stream (stdout or stderr) and appends parsed entries
func processStream(buf *bytes.Buffer, streamType string, entries *[]logs.LogEntry) error {
	if buf == nil || buf.Len() == 0 {
		return nil
	}

	scanner := bufio.NewScanner(buf)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024) // 64KB buffer, 1MB max

	// Pre-allocate with estimated capacity
	newEntries := make([]logs.LogEntry, 0, 100)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		entry := parseLogLine(line, streamType)
		newEntries = append(newEntries, entry)
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scanning %s: %w", streamType, err)
	}

	*entries = append(*entries, newEntries...)
	return nil
}

// ScanStream reads r line by line and calls emit for each non-empty line,
// parsed into a LogEntry via the same logic as the buffered parser. It is the
// streaming counterpart of processStream: producers that follow a container's
// output live feed the reader here and publish the emitted entries in small
// batches, rather than buffering the whole stream to EOF first.
//
// It blocks until r reaches EOF (the follow stream closes when the container
// stops) or a read error occurs, returning any read error.
//
// Lines longer than maxStreamLineSize are truncated rather than aborting the
// scan. A bufio.Scanner stops reading at its first oversized token, and with
// the unbuffered pipes the Docker executor feeds in here that wedges StdCopy,
// stalls the step for the whole drain timeout and drops the log tail.
func ScanStream(r io.Reader, streamType string, emit func(logs.LogEntry)) error {
	if r == nil {
		return nil
	}

	br := bufio.NewReaderSize(r, 64*1024)
	var line []byte
	for {
		chunk, err := br.ReadSlice('\n')
		// A line that fits the reader's buffer is used straight from it; only
		// longer ones are staged in line. chunk is bufio's own memory, so it
		// must never become line's backing array.
		full := chunk
		if err == bufio.ErrBufferFull || len(line) > 0 {
			if room := maxStreamLineSize - len(line); room > 0 {
				line = append(line, chunk[:min(len(chunk), room)]...)
			}
			if err == bufio.ErrBufferFull {
				continue
			}
			full = line
		}
		if text := strings.TrimSpace(string(full)); text != "" {
			emit(parseLogLine(text, streamType))
		}
		line = line[:0]
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return fmt.Errorf("scanning %s stream: %w", streamType, err)
		}
	}
}

// maxStreamLineSize caps one streamed log line; the rest of the line is dropped.
const maxStreamLineSize = 1024 * 1024

// parseLogLine parses a single log line into a structured LogEntry.
// It handles both structured application logs and simple container logs.
func parseLogLine(line, stream string) logs.LogEntry {
	var timestamp time.Time
	var message string

	// Regex pattern matches structured logs with format: time="..." level="..." msg="..."
	if logLineRegex.MatchString(line) {
		// Handle structured application logs with embedded timestamps
		timestamp, message = parseStructuredLog(line)
	} else {
		// Handle simple container logs with timestamp prefix
		timestamp, message = parseSimpleLog(line)
	}

	return logs.LogEntry{
		Timestamp:    timestamp,
		Message:      message,
		OutputStream: stream,
	}
}

// parseStructuredLog extracts timestamp and message from structured logs
func parseStructuredLog(line string) (time.Time, string) {
	// Split: container timestamp | application time="..." | level + msg
	parts := strings.SplitN(line, " ", 3)
	if len(parts) < 3 {
		return time.Now(), line
	}

	// Extract application timestamp from time="..." format
	timeStr := strings.TrimSuffix(strings.TrimPrefix(parts[1], `time="`), `"`)
	timestamp, err := time.Parse(time.RFC3339Nano, timeStr)
	if err != nil {
		slog.Debug("Failed to parse structured log timestamp",
			"timestamp", timeStr,
			"error", err)
		timestamp = time.Now()
	}

	return timestamp, parts[2]
}

// parseSimpleLog extracts timestamp and message from simple container logs
func parseSimpleLog(line string) (time.Time, string) {
	parts := strings.SplitN(line, " ", 2)
	if len(parts) == 0 {
		return time.Now(), ""
	}

	timestamp, err := time.Parse(time.RFC3339Nano, parts[0])
	if err != nil {
		slog.Debug("Failed to parse timestamp, using current time",
			"timestamp", parts[0],
			"error", err)
		return time.Now(), line
	}

	message := ""
	if len(parts) > 1 {
		message = parts[1]
	}

	return timestamp, message
}
