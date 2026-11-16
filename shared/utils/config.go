package utils

import (
	"fmt"
	"log/slog"
	"math"
	"net/url"
	"regexp"
	"strconv"
	"strings"

	"github.com/caarlos0/env/v11"
	"github.com/joho/godotenv"
)

const (
	// Memory unit multipliers
	gigabyte = 1024 * 1024 * 1024
	megabyte = 1024 * 1024
)

// ExecutorConfig holds job executor configuration.
type ExecutorConfig struct {
	// Executor is the executor to use for running the jobs (default: docker)
	// Possible values: docker, k8s
	Executor string `env:"HADES_EXECUTOR,notEmpty" envDefault:"docker"`
}

// LoadConfig loads configuration from environment variables and .env file.
// It will log warnings if the .env file cannot be loaded, but will not fail
// since environment variables may be provided directly.
//
// The config struct itself is never logged, as it may contain secrets
// (auth keys, passwords). Callers should treat a returned error as fatal.
func LoadConfig(cfg interface{}) error {
	// Try to load .env file, but don't fail if it doesn't exist
	if err := godotenv.Load(); err != nil {
		slog.With("error", err).Warn("Error loading .env file")
	}

	// Parse environment variables into config struct
	if err := env.Parse(cfg); err != nil {
		slog.With("error", err).Error("Error parsing environment variables")
		return fmt.Errorf("failed to parse environment variables: %w", err)
	}

	slog.Debug("Configuration loaded")
	return nil
}

// ParseMemoryLimit parses a memory limit string (e.g., "1G", "512M") and returns
// the value in bytes. Supported units are G/g (gigabytes) and M/m (megabytes).
func ParseMemoryLimit(limit string) (int64, error) {
	if len(limit) < 2 {
		return 0, fmt.Errorf("invalid memory limit format: %s", limit)
	}

	unit := limit[len(limit)-1:]
	number := limit[:len(limit)-1]

	value, err := strconv.ParseInt(number, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid memory value %q: %w", number, err)
	}

	if value < 0 {
		return 0, fmt.Errorf("memory limit cannot be negative: %d", value)
	}

	var factor int64
	switch strings.ToUpper(unit) {
	case "G":
		factor = gigabyte
	case "M":
		factor = megabyte
	default:
		return 0, fmt.Errorf("unsupported memory unit %q: must be G (gigabytes) or M (megabytes)", unit)
	}
	// A huge value would wrap around to a small or negative byte count.
	if value > math.MaxInt64/factor {
		return 0, fmt.Errorf("memory limit too large: %s", limit)
	}
	return value * factor, nil
}

// namedNetworkPattern matches a valid Docker network name: it must start with an
// alphanumeric character and may contain letters, digits, and the separators
// "_", ".", "-". This rejects whitespace and control characters.
var namedNetworkPattern = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.-]*$`)

// ValidateNetworkMode checks that network is one of the fixed Docker network
// modes ("none", "bridge", "host", "default") or a valid named network. An empty
// string is valid and means "executor default".
func ValidateNetworkMode(network string) error {
	if network == "" {
		return nil
	}
	switch network {
	case "none", "bridge", "host", "default":
		return nil
	}
	if !namedNetworkPattern.MatchString(network) {
		return fmt.Errorf("invalid network mode: must be none, bridge, host, default, or a valid network name")
	}
	return nil
}

// ValidateCallbackURL checks that raw is a well-formed absolute http or https
// URL with a host. It is used both when a job is submitted (fail-fast) and
// before the log manager forwards logs to the URL (defense in depth).
func ValidateCallbackURL(raw string) error {
	// Error messages intentionally omit the raw URL: a rejected value may carry
	// secrets in its query or fragment, and these errors are logged and returned
	// to the caller.
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("callback URL is not parseable")
	}
	if !u.IsAbs() {
		return fmt.Errorf("callback URL must be absolute")
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("callback URL scheme must be http or https")
	}
	if u.Host == "" {
		return fmt.Errorf("callback URL must include a host")
	}
	return nil
}
