package dashboard

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"golang.org/x/crypto/bcrypt"
)

const (
	sessionCookieName = "hades_dashboard_session"
	minSecretLen      = 32
	// login lockout parameters
	maxFailedAttempts = 5
	lockoutWindow     = 15 * time.Minute
	// maxFailureEntries bounds the lockout map so a client rotating its source
	// key cannot grow it without limit (memory DoS).
	maxFailureEntries = 4096
	// maxLoginBodyBytes caps the login request body.
	maxLoginBodyBytes = 4 << 10
)

// authenticator verifies credentials and mints/validates signed session cookies.
// The cookie is a stdlib HMAC-SHA256 signed token: base64url(payload).base64url(mac).
type authenticator struct {
	username       string
	passwordHash   []byte
	secret         []byte
	ttl            time.Duration
	insecureCookie bool

	mu       sync.Mutex
	failures map[string]*failureState
}

type failureState struct {
	count int
	until time.Time
}

type sessionClaims struct {
	User string `json:"u"`
	Exp  int64  `json:"exp"`
}

func newAuthenticator(cfg Config) (*authenticator, error) {
	// A disabled dashboard still constructs (routes return 503), so tolerate
	// empty credentials here and gate on Config.Enabled() at the routing layer.
	if cfg.Enabled() && len(cfg.SessionSecret) < minSecretLen {
		return nil, fmt.Errorf("DASHBOARD_SESSION_SECRET must be at least %d characters", minSecretLen)
	}
	ttl := cfg.SessionTTL
	if ttl <= 0 {
		ttl = 12 * time.Hour
	}
	return &authenticator{
		username:       cfg.Username,
		passwordHash:   []byte(cfg.PasswordHash),
		secret:         []byte(cfg.SessionSecret),
		ttl:            ttl,
		insecureCookie: cfg.InsecureCookie,
		failures:       make(map[string]*failureState),
	}, nil
}

// verifyCredentials checks a username/password using constant-time comparisons.
func (a *authenticator) verifyCredentials(username, password string) bool {
	userOK := subtle.ConstantTimeCompare([]byte(username), []byte(a.username)) == 1
	passOK := bcrypt.CompareHashAndPassword(a.passwordHash, []byte(password)) == nil
	// Evaluate both regardless of the username result to avoid leaking which
	// factor failed via timing.
	return userOK && passOK
}

// reserve counts a login attempt for key before its password is verified, so
// parallel requests cannot evaluate more than maxFailedAttempts guesses per
// window. It returns false when key is locked out. A successful login clears
// the count via recordSuccess.
func (a *authenticator) reserve(key string) bool {
	now := time.Now()
	a.mu.Lock()
	defer a.mu.Unlock()

	// Opportunistically drop expired entries so the map cannot grow without
	// bound when a client rotates its source key.
	for k, v := range a.failures {
		if now.After(v.until) {
			delete(a.failures, k)
		}
	}

	fs := a.failures[key]
	if fs == nil {
		if len(a.failures) >= maxFailureEntries {
			// Hard cap reached (all entries still within the window): refuse to
			// track new keys rather than grow unbounded. Existing locked-out
			// keys keep their state.
			return true
		}
		fs = &failureState{}
		a.failures[key] = fs
	} else if fs.count >= maxFailedAttempts {
		return false
	}
	fs.count++
	fs.until = now.Add(lockoutWindow)
	return true
}

func (a *authenticator) recordSuccess(key string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	delete(a.failures, key)
}

// issue creates a signed session token valid for the configured TTL.
func (a *authenticator) issue(username string) (string, time.Time, error) {
	exp := time.Now().Add(a.ttl)
	claims := sessionClaims{User: username, Exp: exp.Unix()}
	body, err := json.Marshal(claims)
	if err != nil {
		return "", time.Time{}, err
	}
	payload := base64.RawURLEncoding.EncodeToString(body)
	mac := a.sign(payload)
	return payload + "." + mac, exp, nil
}

// validate verifies a token's signature and expiry and returns the username.
func (a *authenticator) validate(token string) (string, error) {
	parts := strings.SplitN(token, ".", 2)
	if len(parts) != 2 {
		return "", errors.New("malformed session token")
	}
	payload, mac := parts[0], parts[1]
	expected := a.sign(payload)
	if subtle.ConstantTimeCompare([]byte(mac), []byte(expected)) != 1 {
		return "", errors.New("invalid session signature")
	}
	body, err := base64.RawURLEncoding.DecodeString(payload)
	if err != nil {
		return "", errors.New("invalid session encoding")
	}
	var claims sessionClaims
	if err := json.Unmarshal(body, &claims); err != nil {
		return "", errors.New("invalid session payload")
	}
	if time.Now().Unix() >= claims.Exp {
		return "", errors.New("session expired")
	}
	return claims.User, nil
}

func (a *authenticator) sign(payload string) string {
	m := hmac.New(sha256.New, a.secret)
	m.Write([]byte(payload))
	return base64.RawURLEncoding.EncodeToString(m.Sum(nil))
}

// --- Gin handlers/middleware ---

type loginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

func (s *Server) handleLogin(c *gin.Context) {
	key := c.ClientIP()
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, maxLoginBodyBytes)
	var req loginRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request"})
		return
	}

	if !s.auth.reserve(key) {
		c.JSON(http.StatusTooManyRequests, gin.H{"error": "too many failed attempts, try again later"})
		return
	}

	if !s.auth.verifyCredentials(req.Username, req.Password) {
		// Deliberately do not log the submitted username (avoid recording
		// credential-guessing input / PII in logs).
		slog.Warn("Dashboard login failed", "client_ip", key)
		c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid credentials"})
		return
	}
	s.auth.recordSuccess(key)

	token, exp, err := s.auth.issue(req.Username)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to create session"})
		return
	}
	s.setSessionCookie(c, token, exp)
	c.JSON(http.StatusOK, gin.H{"username": req.Username, "version": s.cfg.Version})
}

func (s *Server) handleLogout(c *gin.Context) {
	s.clearSessionCookie(c)
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

func (s *Server) handleSession(c *gin.Context) {
	user, _ := c.Get("dashboardUser")
	c.JSON(http.StatusOK, gin.H{"username": user, "version": s.cfg.Version})
}

// authMiddleware rejects requests without a valid session cookie.
func (s *Server) authMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		cookie, err := c.Cookie(sessionCookieName)
		if err != nil {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "authentication required"})
			return
		}
		user, err := s.auth.validate(cookie)
		if err != nil {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "invalid or expired session"})
			return
		}
		c.Set("dashboardUser", user)
		c.Next()
	}
}

func (s *Server) setSessionCookie(c *gin.Context, token string, exp time.Time) {
	http.SetCookie(c.Writer, &http.Cookie{
		Name:     sessionCookieName,
		Value:    token,
		Path:     "/",
		Expires:  exp,
		MaxAge:   int(time.Until(exp).Seconds()),
		HttpOnly: true,
		Secure:   !s.auth.insecureCookie,
		SameSite: http.SameSiteStrictMode,
	})
}

func (s *Server) clearSessionCookie(c *gin.Context) {
	http.SetCookie(c.Writer, &http.Cookie{
		Name:     sessionCookieName,
		Value:    "",
		Path:     "/",
		MaxAge:   -1,
		HttpOnly: true,
		Secure:   !s.auth.insecureCookie,
		SameSite: http.SameSiteStrictMode,
	})
}
