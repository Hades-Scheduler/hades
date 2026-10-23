package nats

import (
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Payloads must expire so the bucket cannot grow forever, but only well after
// the 24h queue retention: they are still read when a long job finishes and
// while its status webhook is retried.
func TestJobsKVConfigTTL(t *testing.T) {
	cfg := JobsKVConfig()
	assert.Equal(t, JobsBucket, cfg.Bucket)
	assert.Equal(t, jobPayloadTTL, cfg.TTL)
	assert.Greater(t, cfg.TTL, 24*time.Hour)
}

// CreateOrUpdateKeyValue rewrites the bucket to the caller's config, so a call
// site that builds its own KeyValueConfig for HADES_JOBS would silently undo the
// TTL whenever that service starts last. Scan the workspace for one.
func TestNoHandRolledJobsKVConfig(t *testing.T) {
	root := filepath.Join("..", "..")
	literal := regexp.MustCompile(`KeyValueConfig\{[^}]*"HADES_JOBS"`)
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch d.Name() {
			case "node_modules", ".git", "vendor":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		src, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		assert.False(t, literal.Match(src), "%s builds its own HADES_JOBS KeyValueConfig; use JobsKVConfig()", path)
		return nil
	})
	require.NoError(t, err)
}
