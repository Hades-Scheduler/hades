package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseMemoryLimit_Gigabytes(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected int64
	}{
		{"1 gigabyte uppercase", "1G", 1073741824},
		{"2 gigabytes lowercase", "2g", 2147483648},
		{"10 gigabytes", "10G", 10737418240},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseMemoryLimit(tt.input)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, got)
		})
	}
}

func TestParseMemoryLimit_Megabytes(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected int64
	}{
		{"1 megabyte uppercase", "1M", 1048576},
		{"2 megabytes lowercase", "2m", 2097152},
		{"512 megabytes", "512M", 536870912},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseMemoryLimit(tt.input)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, got)
		})
	}
}

func TestParseMemoryLimit_UnknownUnit(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"terabyte", "1T"},
		{"kilobyte", "1K"},
		{"byte", "1B"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseMemoryLimit(tt.input)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "unsupported memory unit")
		})
	}
}

func TestValidateCallbackURL_Valid(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"http", "http://localhost:8082/adapter/logs"},
		{"https", "https://example.com/adapter/logs"},
		{"https with port", "https://host.example.com:8443/path"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.NoError(t, ValidateCallbackURL(tt.input))
		})
	}
}

func TestValidateCallbackURL_Invalid(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expectedErr string
	}{
		{"empty", "", "must be absolute"},
		{"relative", "/adapter/logs", "must be absolute"},
		{"missing scheme", "example.com/logs", "must be absolute"},
		{"ftp scheme", "ftp://example.com/logs", "scheme must be http or https"},
		{"file scheme", "file:///etc/passwd", "scheme must be http or https"},
		{"no host", "http:///path", "must include a host"},
		{"javascript", "javascript:alert(1)", "scheme must be http or https"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateCallbackURL(tt.input)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.expectedErr)
		})
	}
}

func TestParseMemoryLimit_InvalidInput(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expectedErr string
	}{
		{"invalid number", "abcM", "invalid memory value"},
		{"empty string", "", "invalid memory limit format"},
		{"single character", "G", "invalid memory limit format"},
		{"negative value", "-5G", "memory limit cannot be negative"},
		{"no unit", "123", "unsupported memory unit"},
		{"overflowing gigabytes", "9223372036G", "memory limit too large"},
		{"overflowing megabytes", "9223372036854775807M", "memory limit too large"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseMemoryLimit(tt.input)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.expectedErr)
		})
	}
}

func TestValidateNetworkMode(t *testing.T) {
	valid := []string{"", "none", "bridge", "host", "default", "my-net", "hades_net.1", "artemis-build-network"}
	for _, n := range valid {
		t.Run("valid/"+n, func(t *testing.T) {
			assert.NoError(t, ValidateNetworkMode(n))
		})
	}

	invalid := []string{" ", "no ne", "net work", "-leading", ".dot", "bad/slash", "tab\there"}
	for _, n := range invalid {
		t.Run("invalid/"+n, func(t *testing.T) {
			assert.Error(t, ValidateNetworkMode(n))
		})
	}
}
