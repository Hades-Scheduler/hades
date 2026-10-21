package log

import (
	"strings"
	"testing"

	logs "github.com/hades-scheduler/hades/shared/buildlogs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestScanStream_EmitsNonEmptyLinesWithStreamType(t *testing.T) {
	input := "2023-01-01T00:00:00Z first line\n\n2023-01-01T00:00:01Z second line\n   \n2023-01-01T00:00:02Z third line\n"

	var got []logs.LogEntry
	err := ScanStream(strings.NewReader(input), StreamStderr, func(e logs.LogEntry) {
		got = append(got, e)
	})
	require.NoError(t, err)

	require.Len(t, got, 3, "empty and whitespace-only lines are skipped")
	assert.Equal(t, "first line", got[0].Message)
	assert.Equal(t, "second line", got[1].Message)
	assert.Equal(t, "third line", got[2].Message)
	for _, e := range got {
		assert.Equal(t, StreamStderr, e.OutputStream)
	}
}

func TestScanStream_ParsesStructuredAndSimpleLines(t *testing.T) {
	// A structured application log line and a simple container line should be
	// parsed exactly as the buffered parser would via parseLogLine.
	input := `2023-01-01T00:00:00Z time="2023-06-01T12:00:00Z" level="info" msg="structured message"` + "\n" +
		"2023-01-01T00:00:01Z plain message\n"

	var got []logs.LogEntry
	err := ScanStream(strings.NewReader(input), StreamStdout, func(e logs.LogEntry) {
		got = append(got, e)
	})
	require.NoError(t, err)
	require.Len(t, got, 2)

	assert.Equal(t, `level="info" msg="structured message"`, got[0].Message)
	assert.Equal(t, "2023-06-01T12:00:00Z", got[0].Timestamp.UTC().Format("2006-01-02T15:04:05Z"))
	assert.Equal(t, "plain message", got[1].Message)
}

func TestScanStream_NilReader(t *testing.T) {
	called := false
	err := ScanStream(nil, StreamStdout, func(logs.LogEntry) { called = true })
	require.NoError(t, err)
	assert.False(t, called)
}

func TestScanStream_TruncatesOversizedLineAndKeepsReading(t *testing.T) {
	long := "2023-01-01T00:00:00Z " + strings.Repeat("x", 3*maxStreamLineSize)
	input := long + "\n2023-01-01T00:00:01Z after\nno newline at EOF"

	var got []logs.LogEntry
	err := ScanStream(strings.NewReader(input), StreamStdout, func(e logs.LogEntry) {
		got = append(got, e)
	})
	require.NoError(t, err)
	require.Len(t, got, 3, "an oversized line must not stop the scan")
	assert.Len(t, got[0].Message, maxStreamLineSize-len("2023-01-01T00:00:00Z "))
	assert.Equal(t, "after", got[1].Message)
	assert.Equal(t, "no newline at EOF", got[2].Message)
}
