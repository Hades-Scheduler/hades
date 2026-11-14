// Package redact removes sensitive data from job payloads before they are
// surfaced to operators (the dashboard) or written to logs.
//
// Hades jobs carry free-form Metadata maps (both job-level and per-step) that
// are injected verbatim into containers as environment variables. In practice
// these hold credentials - git tokens, registry passwords, connection strings -
// so their values must never leave the process unmasked.
//
// Two strategies are provided:
//
//   - Drop: remove every metadata entry entirely (keys and values). This mirrors
//     the historical logging behaviour and is the safest option.
//   - Redactor: keep metadata keys but mask sensitive values. A value is masked
//     when its key matches a denylist of sensitive name patterns, or when the
//     value itself looks like a secret (credentials embedded in a URL, a PEM
//     block, a JWT, or a high-entropy blob). In ModeAll every value is masked
//     regardless. Keeping keys lets operators see which variables a job defines
//     without exposing the secret values.
package redact

import (
	"fmt"
	"math"
	"regexp"
	"sort"
	"strings"

	"github.com/hades-scheduler/hades/shared/payload"
)

// Mask is the placeholder shown in place of a redacted value.
const Mask = "••••••"

// Mode selects how aggressively values are masked.
type Mode string

const (
	// ModeSmart masks values whose key matches the sensitive-key denylist or
	// whose value matches a secret heuristic. This is the default.
	ModeSmart Mode = "smart"
	// ModeAll masks every metadata value regardless of key or content.
	ModeAll Mode = "all"
)

// DefaultKeyPattern matches metadata keys that conventionally hold secrets.
// It is case-insensitive and matches the pattern anywhere in the key.
const DefaultKeyPattern = `(?i)(token|passwd|password|pwd|secret|api[_-]?key|access[_-]?key|credential|auth|private|cert|signing|session)`

// Config configures a Redactor. The zero value is not valid; use New or Default.
type Config struct {
	// Mode selects the masking strategy. Defaults to ModeSmart when empty.
	Mode Mode `env:"SECRET_REDACT_MODE" envDefault:"smart"`
	// KeyPattern is a case-insensitive regular expression; metadata keys that
	// match are masked. Defaults to DefaultKeyPattern when empty.
	KeyPattern string `env:"SECRET_KEY_PATTERNS"`
}

// Redactor masks sensitive metadata values according to its configuration.
// A Redactor is safe for concurrent use.
type Redactor struct {
	mode  Mode
	keyRe *regexp.Regexp
}

var (
	// credentialsInURL matches a scheme://user:password@host style URL.
	credentialsInURL = regexp.MustCompile(`[a-zA-Z][a-zA-Z0-9+.\-]*://[^/\s:@]+:[^/\s@]+@`)
	// jwtLike matches a three-segment base64url token (header.payload.signature).
	jwtLike = regexp.MustCompile(`^[A-Za-z0-9_-]{6,}\.[A-Za-z0-9_-]{6,}\.[A-Za-z0-9_-]{6,}$`)
	// tokenCharset matches strings made only of base64/hex/token characters.
	tokenCharset = regexp.MustCompile(`^[A-Za-z0-9+/=_\-]+$`)
)

// New builds a Redactor from cfg, compiling the key pattern. It returns an
// error if the key pattern is not a valid regular expression.
func New(cfg Config) (*Redactor, error) {
	mode := cfg.Mode
	if mode == "" {
		mode = ModeSmart
	}
	pattern := cfg.KeyPattern
	if pattern == "" {
		pattern = DefaultKeyPattern
	}
	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, fmt.Errorf("compiling secret key pattern %q: %w", pattern, err)
	}
	return &Redactor{mode: mode, keyRe: re}, nil
}

// Default returns a Redactor with the built-in smart configuration.
func Default() *Redactor {
	r, err := New(Config{Mode: ModeSmart})
	if err != nil {
		// DefaultKeyPattern is a compile-time constant known to be valid.
		panic(fmt.Sprintf("redact: default config failed to compile: %v", err))
	}
	return r
}

// Value reports the display value for a single metadata entry and whether it
// was masked. Empty values are left untouched (nothing to hide).
func (r *Redactor) Value(key, value string) (string, bool) {
	if value == "" {
		return value, false
	}
	if r.mode == ModeAll || r.keyRe.MatchString(key) || looksLikeSecret(value) {
		return Mask, true
	}
	return value, false
}

// Metadata returns a redacted copy of m. The input map is never modified.
func (r *Redactor) Metadata(m map[string]string) map[string]string {
	if m == nil {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		masked, _ := r.Value(k, v)
		out[k] = masked
	}
	return out
}

// Payload returns a deep copy of job with all job-level and step-level metadata
// redacted, and secrets scrubbed from each step's Script. The input payload and
// its slices/maps are never modified.
func (r *Redactor) Payload(job payload.QueuePayload) payload.QueuePayload {
	secrets := r.maskedValues(job.Metadata, nil)
	for _, st := range job.Steps {
		secrets = r.maskedValues(st.Metadata, secrets)
	}
	// Longest first so a secret that contains another one is replaced whole.
	sort.Slice(secrets, func(i, j int) bool { return len(secrets[i]) > len(secrets[j]) })
	pairs := make([]string, 0, 2*len(secrets))
	for _, v := range secrets {
		pairs = append(pairs, v, Mask)
	}
	scrub := strings.NewReplacer(pairs...)

	job.Metadata = r.Metadata(job.Metadata)
	steps := make([]payload.Step, len(job.Steps))
	copy(steps, job.Steps)
	for i := range steps {
		steps[i].Metadata = r.Metadata(steps[i].Metadata)
		steps[i].Script = r.Text(scrub.Replace(steps[i].Script))
	}
	job.Steps = steps
	return job
}

// minScriptSecretLen is the shortest masked metadata value that is also
// scrubbed from scripts; shorter values ("1", "on") would mangle ordinary text.
const minScriptSecretLen = 4

// maskedValues appends the values of m that Value masks (and that are long
// enough to search for) to dst.
func (r *Redactor) maskedValues(m map[string]string, dst []string) []string {
	for k, v := range m {
		if _, masked := r.Value(k, v); masked && len(v) >= minScriptSecretLen {
			dst = append(dst, v)
		}
	}
	return dst
}

var (
	// authHeader matches an Authorization header or assignment with a Bearer,
	// Basic or Token scheme; group 2 is the credential.
	authHeader = regexp.MustCompile(`(?i)(authorization\s*[:=]\s*["']?(?:bearer|basic|token)\s+)([^\s"';]+)`)
	// urlUserToken matches a token-only URL credential (scheme://token@host).
	urlUserToken = regexp.MustCompile(`([a-zA-Z][a-zA-Z0-9+.\-]*://)([A-Za-z0-9_.\-]{12,})(@)`)
	// spacedSecretFlag matches --password/--token style flags whose value is the
	// next word or quoted string; "--password-stdin" does not match because the flag must end
	// with the keyword.
	spacedSecretFlag = regexp.MustCompile(`(?i)(\s--[\w\-]*?(?:token|secret|passwd|password|pwd|api[_-]?key|credential)\s+)((?:"[^"]*"|'[^']*'|[^\s"';\-][^\s"';]*))`)
	// loginPassword matches -p <secret> only on a registry login command line,
	// where -p is a password; elsewhere it is a path or port flag.
	loginPassword = regexp.MustCompile(`(\b(?:docker|podman|buildah|skopeo|crane|oras)\b[^\n;&|]*?\blogin\b[^\n;&|]*?\s-p\s+)((?:"[^"]*"|'[^']*'|[^\s"';]+))`)
	// mysqlPassword matches the attached -p<secret> form of the mysql tools.
	mysqlPassword = regexp.MustCompile(`(\bmysql(?:dump|admin)?\b[^\n;&|]*?\s-p)((?:"[^"]*"|'[^']*'|[^\s"';]+))`)
	// urlCredentials captures the password segment of a scheme://user:pass@host URL.
	urlCredentials = regexp.MustCompile(`([a-zA-Z][a-zA-Z0-9+.\-]*://[^/\s:@]+:)([^/\s@]+)(@)`)
	// sensitiveAssignment matches KEY=VALUE / KEY: VALUE / export KEY=VALUE where
	// the key looks sensitive; group 3 is the (optionally quoted) value to mask.
	sensitiveAssignment = regexp.MustCompile(`(?i)((?:^|[\s;&|(])(?:export\s+|set\s+|-e\s+)?[\w.\-]*(?:token|secret|passwd|password|pwd|api[_-]?key|access[_-]?key|credential|auth|private|signing|session)[\w.\-]*\s*[:=]\s*)(["']?)([^\s"';]+)`)
	// jwtInline matches a JWT anywhere in a larger string (jwtLike is anchored).
	jwtInline = regexp.MustCompile(`[A-Za-z0-9_-]{6,}\.[A-Za-z0-9_-]{6,}\.[A-Za-z0-9_-]{6,}`)
	// pemBlock matches a whole PEM key/cert block.
	pemBlock = regexp.MustCompile(`(?s)-----BEGIN[^-]*-----.*?-----END[^-]*-----`)
	// longToken matches long unbroken base64/hex/token strings (checked for entropy).
	longToken = regexp.MustCompile(`[A-Za-z0-9+/=_\-]{24,}`)
)

// Text returns s with embedded secrets masked: credentials in URLs, sensitive
// KEY=VALUE assignments, PEM blocks, JWTs, and long high-entropy tokens. It is
// used to scrub step scripts, which routinely embed clone URLs and API keys.
// Structure is preserved so the script stays readable; only secret spans are
// replaced with the mask.
func (r *Redactor) Text(s string) string {
	if s == "" {
		return s
	}
	s = urlCredentials.ReplaceAllString(s, "${1}"+Mask+"${3}")
	s = urlUserToken.ReplaceAllStringFunc(s, func(m string) string {
		sub := urlUserToken.FindStringSubmatch(m)
		if !strings.ContainsAny(sub[2], "0123456789_-") {
			return m
		}
		return sub[1] + Mask + sub[3]
	})
	s = authHeader.ReplaceAllString(s, "${1}"+Mask)
	s = spacedSecretFlag.ReplaceAllString(s, "${1}"+Mask)
	s = loginPassword.ReplaceAllString(s, "${1}"+Mask)
	s = mysqlPassword.ReplaceAllString(s, "${1}"+Mask)
	s = sensitiveAssignment.ReplaceAllString(s, "${1}${2}"+Mask)
	s = pemBlock.ReplaceAllString(s, Mask)
	s = jwtInline.ReplaceAllString(s, Mask)
	s = longToken.ReplaceAllStringFunc(s, func(tok string) string {
		if tok == Mask {
			return tok
		}
		if shannonEntropy(tok) >= 3.5 {
			return Mask
		}
		return tok
	})
	return s
}

// Drop returns a deep copy of job with every metadata map emptied (keys and
// values removed). Use this for log output where key names add no value.
func Drop(job payload.QueuePayload) payload.QueuePayload {
	job.Metadata = map[string]string{}
	steps := make([]payload.Step, len(job.Steps))
	copy(steps, job.Steps)
	for i := range steps {
		steps[i].Metadata = map[string]string{}
	}
	job.Steps = steps
	return job
}

// looksLikeSecret applies content heuristics that catch secrets regardless of
// their key name: credentials embedded in a URL, PEM key blocks, JWTs, and
// long high-entropy tokens.
func looksLikeSecret(value string) bool {
	if credentialsInURL.MatchString(value) {
		return true
	}
	if strings.Contains(value, "-----BEGIN") {
		return true
	}
	if jwtLike.MatchString(value) {
		return true
	}
	// Long, unbroken, high-entropy token strings (e.g. API keys) with no
	// whitespace are almost always secrets.
	if len(value) >= 24 && tokenCharset.MatchString(value) && shannonEntropy(value) >= 3.5 {
		return true
	}
	return false
}

// shannonEntropy returns the Shannon entropy (bits per character) of s.
func shannonEntropy(s string) float64 {
	if s == "" {
		return 0
	}
	var counts [256]float64
	for i := 0; i < len(s); i++ {
		counts[s[i]]++
	}
	n := float64(len(s))
	var entropy float64
	for _, c := range counts {
		if c == 0 {
			continue
		}
		p := c / n
		entropy -= p * math.Log2(p)
	}
	return entropy
}
