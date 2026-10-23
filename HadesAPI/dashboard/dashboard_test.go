package dashboard

import (
	"bufio"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	hades "github.com/hades-scheduler/hades/shared"
	"github.com/hades-scheduler/hades/shared/buildstatus"
	"github.com/hades-scheduler/hades/shared/redact"
	"golang.org/x/crypto/bcrypt"
)

func init() { gin.SetMode(gin.TestMode) }

const testPassword = "s3cret-pass"

// testServer builds a Server without NATS. When enabled it has real credentials.
func testServer(t *testing.T, enabled bool) *Server {
	t.Helper()
	cfg := Config{JobRetention: time.Hour}
	if enabled {
		hash, err := bcrypt.GenerateFromPassword([]byte(testPassword), bcrypt.MinCost)
		if err != nil {
			t.Fatal(err)
		}
		cfg.Username = "admin"
		cfg.PasswordHash = string(hash)
		cfg.SessionSecret = "0123456789abcdef-0123456789abcdef-secret"
	}
	auth, err := newAuthenticator(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return &Server{
		cfg:      cfg,
		redactor: redact.Default(),
		tracker:  newTracker(cfg.JobRetention),
		hub:      newHub(),
		auth:     auth,
		http:     &http.Client{Timeout: 2 * time.Second},
	}
}

func newRouter(s *Server) *gin.Engine {
	r := gin.New()
	s.RegisterRoutes(r)
	return r
}

// login performs a login and returns the session cookie.
func login(t *testing.T, r *gin.Engine) *http.Cookie {
	t.Helper()
	body := `{"username":"admin","password":"` + testPassword + `"}`
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/login", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("login failed: %d %s", w.Code, w.Body.String())
	}
	for _, c := range w.Result().Cookies() {
		if c.Name == sessionCookieName {
			return c
		}
	}
	t.Fatal("no session cookie set")
	return nil
}

func TestDisabledDashboardReturns503(t *testing.T) {
	r := newRouter(testServer(t, false))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/jobs", nil))
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", w.Code)
	}
}

func TestAuthRequired(t *testing.T) {
	r := newRouter(testServer(t, true))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/jobs", nil))
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 without cookie, got %d", w.Code)
	}
}

func TestLoginWrongPassword(t *testing.T) {
	r := newRouter(testServer(t, true))
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/login", strings.NewReader(`{"username":"admin","password":"wrong"}`))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", w.Code)
	}
}

func TestLoginThenSessionAndLogout(t *testing.T) {
	s := testServer(t, true)
	r := newRouter(s)
	cookie := login(t, r)

	// /api/session returns the username with a valid cookie.
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/session", nil)
	req.AddCookie(cookie)
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), "admin") {
		t.Fatalf("session check failed: %d %s", w.Code, w.Body.String())
	}

	// Logout clears the cookie.
	w = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/api/logout", nil)
	req.AddCookie(cookie)
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("logout failed: %d", w.Code)
	}
}

func TestLoginAndSessionReturnVersion(t *testing.T) {
	s := testServer(t, true)
	s.cfg.Version = "1.2.3"
	r := newRouter(s)

	// Login response carries the deployed version.
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/login", strings.NewReader(`{"username":"admin","password":"`+testPassword+`"}`))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("login failed: %d %s", w.Code, w.Body.String())
	}
	var loginResp struct {
		Version string `json:"version"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &loginResp); err != nil {
		t.Fatal(err)
	}
	if loginResp.Version != "1.2.3" {
		t.Fatalf("login version = %q, want 1.2.3", loginResp.Version)
	}

	var cookie *http.Cookie
	for _, c := range w.Result().Cookies() {
		if c.Name == sessionCookieName {
			cookie = c
		}
	}

	// Session response carries it too.
	w = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/api/session", nil)
	req.AddCookie(cookie)
	r.ServeHTTP(w, req)
	var sessionResp struct {
		Version string `json:"version"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &sessionResp); err != nil {
		t.Fatal(err)
	}
	if sessionResp.Version != "1.2.3" {
		t.Fatalf("session version = %q, want 1.2.3", sessionResp.Version)
	}
}

func TestExpiredSessionRejected(t *testing.T) {
	s := testServer(t, true)
	s.auth.ttl = -time.Minute // issue already-expired tokens
	token, _, err := s.auth.issue("admin")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.auth.validate(token); err == nil {
		t.Fatal("expected expired token to be rejected")
	}
}

func TestTamperedSessionRejected(t *testing.T) {
	s := testServer(t, true)
	token, _, _ := s.auth.issue("admin")
	if _, err := s.auth.validate(token + "x"); err == nil {
		t.Fatal("expected tampered token to be rejected")
	}
}

func TestLoginLockout(t *testing.T) {
	s := testServer(t, true)
	r := newRouter(s)
	for i := 0; i < maxFailedAttempts; i++ {
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/api/login", strings.NewReader(`{"username":"admin","password":"wrong"}`))
		req.Header.Set("Content-Type", "application/json")
		r.ServeHTTP(w, req)
	}
	// Next attempt (even correct) is locked out.
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/login", strings.NewReader(`{"username":"admin","password":"`+testPassword+`"}`))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)
	if w.Code != http.StatusTooManyRequests {
		t.Fatalf("expected 429 after lockout, got %d", w.Code)
	}
}

func TestListJobsReflectsTracker(t *testing.T) {
	s := testServer(t, true)
	r := newRouter(s)
	s.tracker.enqueue("job-1", "build", 3, hades.HighPriority)
	s.tracker.observe("job-1", buildstatus.StatusRunning, "")

	cookie := login(t, r)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/jobs", nil)
	req.AddCookie(cookie)
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("list jobs: %d %s", w.Code, w.Body.String())
	}
	var resp struct {
		Jobs []JobSummary `json:"jobs"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if len(resp.Jobs) != 1 || resp.Jobs[0].ID != "job-1" || resp.Jobs[0].Status != "Running" {
		t.Fatalf("unexpected jobs: %+v", resp.Jobs)
	}
	if resp.Jobs[0].Priority != "high" {
		t.Fatalf("expected high priority, got %q", resp.Jobs[0].Priority)
	}
}

const testJobID = "11111111-2222-3333-4444-555555555555"

func TestJobLogsProxy(t *testing.T) {
	// Stub log manager.
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if !strings.HasSuffix(req.URL.Path, "/jobs/"+testJobID+"/logs") {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"logs":[]}`))
	}))
	defer stub.Close()

	s := testServer(t, true)
	s.cfg.LogManagerURL = stub.URL
	r := newRouter(s)
	cookie := login(t, r)

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/jobs/"+testJobID+"/logs", nil)
	req.AddCookie(cookie)
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), "logs") {
		t.Fatalf("logs proxy: %d %s", w.Code, w.Body.String())
	}
}

func TestJobLogsProxyUnavailable(t *testing.T) {
	s := testServer(t, true)
	s.cfg.LogManagerURL = "http://127.0.0.1:0" // unreachable
	r := newRouter(s)
	cookie := login(t, r)

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/jobs/"+testJobID+"/logs", nil)
	req.AddCookie(cookie)
	r.ServeHTTP(w, req)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 when log manager down, got %d", w.Code)
	}
}

// TestStreamSurvivesWriteTimeout proves the SSE handler clears the per-connection
// write deadline: with a short server WriteTimeout the stream still delivers an
// event pushed well after that timeout would have fired.
func TestStreamSurvivesWriteTimeout(t *testing.T) {
	s := testServer(t, true)
	r := newRouter(s)
	cookie := login(t, r)

	srv := httptest.NewUnstartedServer(r)
	srv.Config.WriteTimeout = 200 * time.Millisecond
	srv.Start()
	defer srv.Close()

	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/api/stream", nil)
	req.AddCookie(cookie)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("stream request: %v", err)
	}
	defer resp.Body.Close()

	reader := bufio.NewReader(resp.Body)
	readData := func() (string, error) {
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				return "", err
			}
			if strings.HasPrefix(line, "data: ") {
				return line, nil
			}
		}
	}

	// Initial primed metrics event.
	if _, err := readData(); err != nil {
		t.Fatalf("initial event: %v", err)
	}

	// Wait past the server WriteTimeout, then push an event.
	time.Sleep(400 * time.Millisecond)
	s.hub.broadcast(event{Type: eventJob, Job: JobSummary{ID: "x", Status: "Queued"}})

	done := make(chan error, 1)
	go func() {
		_, err := readData()
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("stream severed after WriteTimeout: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for post-timeout event")
	}
}

func TestSummarizeDurations(t *testing.T) {
	d := summarizeDurations([]int64{100, 200, 300, 400, 500})
	if d.Count != 5 || d.AvgMs != 300 {
		t.Fatalf("unexpected durations: %+v", d)
	}
	if summarizeDurations(nil).Count != 0 {
		t.Fatal("expected empty durations for nil input")
	}
}

func TestSummarizeDurations_P95NearestRank(t *testing.T) {
	// 10 samples 1..10: nearest-rank p95 = ceil(0.95*10)=10th value = 1000.
	ds := []int64{100, 200, 300, 400, 500, 600, 700, 800, 900, 1000}
	if got := summarizeDurations(ds).P95Ms; got != 1000 {
		t.Fatalf("p95 = %d, want 1000", got)
	}
	// 20 samples: ceil(0.95*20)=19th value.
	twenty := make([]int64, 20)
	for i := range twenty {
		twenty[i] = int64((i + 1) * 10)
	}
	if got := summarizeDurations(twenty).P95Ms; got != 190 {
		t.Fatalf("p95(20) = %d, want 190", got)
	}
}

func TestLockoutMapBounded(t *testing.T) {
	s := testServer(t, true)
	// Far more distinct keys than the cap; the map must not grow unbounded.
	for i := 0; i < maxFailureEntries+500; i++ {
		s.auth.recordFailure("key-" + strconv.Itoa(i))
	}
	s.auth.mu.Lock()
	n := len(s.auth.failures)
	s.auth.mu.Unlock()
	if n > maxFailureEntries {
		t.Fatalf("failures map grew to %d, cap is %d", n, maxFailureEntries)
	}
}

func TestJobLogsRejectsNonUUID(t *testing.T) {
	s := testServer(t, true)
	r := newRouter(s)
	cookie := login(t, r)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/jobs/not-a-uuid/logs", nil)
	req.AddCookie(cookie)
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for non-UUID job id, got %d", w.Code)
	}
}

func TestTrackerObserveReason(t *testing.T) {
	tr := newTracker(time.Hour)
	// A reason attached to a Failed transition is surfaced on the summary.
	sum := tr.observe("j", buildstatus.StatusFailed, "ImagePullBackOff: back-off")
	if sum.Status != "Failed" {
		t.Fatalf("status = %q, want Failed", sum.Status)
	}
	if sum.Reason != "ImagePullBackOff: back-off" {
		t.Fatalf("reason = %q, want the ImagePullBackOff message", sum.Reason)
	}
	// A later empty-reason update must not wipe the stored reason.
	sum = tr.observe("j", buildstatus.StatusFailed, "")
	if sum.Reason != "ImagePullBackOff: back-off" {
		t.Fatalf("reason was cleared by an empty update: %q", sum.Reason)
	}
}

func TestTrackerSweep(t *testing.T) {
	tr := newTracker(time.Millisecond)
	tr.observe("j", buildstatus.StatusSucceeded, "")
	// Force updatedAt into the past.
	tr.mu.Lock()
	tr.jobs["j"].updatedAt = time.Now().Add(-time.Hour)
	tr.mu.Unlock()
	tr.sweep()
	if _, ok := tr.get("j"); ok {
		t.Fatal("expected finished job to be swept")
	}
}

func TestTrackerIdentityIsLookedUpOnce(t *testing.T) {
	tr := newTracker(time.Hour)
	tr.enqueue("enqueued", "Named Job", 3, hades.HighPriority)
	if tr.needsIdentity("enqueued") {
		t.Fatal("a job recorded at enqueue already has its identity")
	}

	tr.observe("seen", buildstatus.StatusRunning, "")
	if !tr.needsIdentity("seen") {
		t.Fatal("a job learned from a status event needs a lookup")
	}
	tr.setIdentity("seen", "From KV", 2)
	if tr.needsIdentity("seen") {
		t.Fatal("identity must be remembered after a lookup")
	}
	if got, _ := tr.get("seen"); got.Name != "From KV" || got.StepCount != 2 {
		t.Fatalf("identity not written back: %+v", got)
	}

	// A definitive miss is remembered too.
	tr.observe("gone", buildstatus.StatusRunning, "")
	tr.setIdentity("gone", "", 0)
	if tr.needsIdentity("gone") {
		t.Fatal("a missing payload must not be looked up again")
	}
}

func TestTrackerStatsSinglePass(t *testing.T) {
	tr := newTracker(time.Hour)
	tr.observe("a", buildstatus.StatusRunning, "")
	tr.observe("a", buildstatus.StatusSucceeded, "")
	tr.observe("b", buildstatus.StatusQueued, "")
	tr.observe("c", buildstatus.StatusFailed, "")
	tr.mu.Lock()
	tr.jobs["c"].finishedAt = time.Now().Add(-time.Hour)
	tr.mu.Unlock()

	st := tr.stats(time.Minute)
	if st.counts["Succeeded"] != 1 || st.counts["Queued"] != 1 || st.counts["Failed"] != 1 {
		t.Fatalf("unexpected counts: %v", st.counts)
	}
	if len(st.durations) != 1 {
		t.Fatalf("only jobs with a start and finish have a duration, got %v", st.durations)
	}
	if st.finishedRecently != 1 {
		t.Fatalf("finishedRecently = %d, want 1", st.finishedRecently)
	}
}

func TestTrackerCapEvictsOldest(t *testing.T) {
	tr := newTracker(time.Hour)
	base := time.Now().Add(-time.Hour)
	tr.mu.Lock()
	for i := range maxTrackedJobs + 10 {
		id := strconv.Itoa(i)
		tr.jobs[id] = &jobRecord{id: id, updatedAt: base.Add(time.Duration(i) * time.Millisecond)}
	}
	tr.enforceCapLocked()
	_, oldest := tr.jobs["0"]
	_, newest := tr.jobs[strconv.Itoa(maxTrackedJobs+9)]
	n := len(tr.jobs)
	tr.mu.Unlock()
	if n != maxTrackedJobs || oldest || !newest {
		t.Fatalf("len=%d oldestKept=%v newestKept=%v", n, oldest, newest)
	}
}

// Every client receives the same pre-encoded frame, and the raw log bytes pass
// through unchanged in the shape the SPA parses.
func TestHubBroadcastsEncodedFrame(t *testing.T) {
	h := newHub()
	h.broadcast(event{Type: eventJob}) // no clients: must not panic or block
	a, b := h.add(), h.add()
	raw := json.RawMessage(`{"job_id":"j","container_id":"c","logs":[{"timestamp":"2026-01-01T00:00:00Z","message":"hi","output_stream":"stdout"}]}`)
	h.broadcast(event{Type: eventLog, Log: raw})

	got := <-a
	if string(got) != string(<-b) {
		t.Fatal("clients received different frames")
	}
	var decoded struct {
		Type string          `json:"type"`
		Log  json.RawMessage `json:"log"`
	}
	if err := json.Unmarshal(got, &decoded); err != nil {
		t.Fatalf("frame is not JSON: %v", err)
	}
	if decoded.Type != eventLog || string(decoded.Log) != string(raw) {
		t.Fatalf("unexpected frame: %s", got)
	}
}
