package controller

// Functional tests for the WorkPackages control path.
//
// Written to be runnable BEFORE deploying a rebuilt operator image, so the behaviour that
// produces FedRAMP compliance evidence can be verified without a cluster. They need no
// envtest and no API server: every function under test is pure or speaks HTTP, so
// `go test ./internal/controller/ -run Functional` exercises them in isolation.
//
// The cases are drawn from a real incident on 2026-09-02, when the monthly compliance
// tickets silently did not get created. Two of these tests would have caught it.

import (
	"context"
	"encoding/base64"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const monthlyFirstOfMonth = "0 0 1 * *"

// --------------------------------------------------------------------------------------
// Schedule parsing and next-run calculation
// --------------------------------------------------------------------------------------

func TestFunctionalParseSchedule(t *testing.T) {
	cases := []struct {
		name     string
		schedule string
		valid    bool
	}{
		{"monthly first of month", monthlyFirstOfMonth, true},
		{"annual february", "0 0 1 2 *", true},
		{"quarterly", "0 0 1 1,4,7,10 *", true},
		{"every 15 minutes", "*/15 * * * *", true},
		{"empty", "", false},
		{"too few fields", "0 0 1", false},
		{"nonsense", "not-a-cron", false},
		{"month out of range", "0 0 1 13 *", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parseSchedule(tc.schedule)
			if tc.valid && err != nil {
				t.Fatalf("expected %q to parse, got error: %v", tc.schedule, err)
			}
			if !tc.valid && err == nil {
				t.Fatalf("expected %q to be rejected, but it parsed", tc.schedule)
			}
		})
	}
}

func TestFunctionalCalculateNextRunTime(t *testing.T) {
	// A monthly schedule evaluated mid-month must land on the 1st of the NEXT month.
	from := time.Date(2026, time.September, 15, 12, 0, 0, 0, time.UTC)
	next, err := calculateNextRunTime(monthlyFirstOfMonth, from)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := time.Date(2026, time.October, 1, 0, 0, 0, 0, time.UTC)
	if !next.Equal(want) {
		t.Fatalf("next run = %s, want %s", next, want)
	}

	if _, err := calculateNextRunTime("not-a-cron", from); err == nil {
		t.Fatal("expected an error for an invalid schedule, got nil")
	}
}

// TestFunctionalFailureSkipsAnEntireCycle documents a REAL DEFECT rather than asserting
// desirable behaviour, so it must be read before being "fixed".
//
// updateFailedStatus computes the next attempt with calculateNextRunTime(schedule, now) -
// the next SCHEDULED occurrence, not a retry - and logs it under the key "nextRetry".
// For a monthly schedule a failure on the 1st therefore waits a full month.
//
// That is exactly what happened on 2026-09-01: eight monthly compliance WorkPackages
// failed (the database had lost md5(), so every work-package API call 500'd), each set
// nextRunTime to 2026-10-01, and September's evidence would simply not have existed.
//
// If the controller is changed to retry with a short backoff, THIS TEST SHOULD FAIL and
// be updated deliberately. Do not delete it to make a build green.
func TestFunctionalFailureSkipsAnEntireCycle(t *testing.T) {
	failedAt := time.Date(2026, time.September, 1, 0, 43, 0, 0, time.UTC)

	next, err := calculateNextRunTime(monthlyFirstOfMonth, failedAt)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	gap := next.Sub(failedAt)
	if gap < 29*24*time.Hour {
		t.Fatalf("expected the post-failure wait to be about a month (current behaviour), got %s", gap)
	}
	if next.Month() != time.October || next.Day() != 1 {
		t.Fatalf("expected the next attempt on 1 October, got %s", next)
	}
}

// --------------------------------------------------------------------------------------
// shouldRunNow
// --------------------------------------------------------------------------------------

func TestFunctionalShouldRunNow(t *testing.T) {
	longAgo := metav1.NewTime(time.Now().Add(-90 * 24 * time.Hour))
	justNow := metav1.NewTime(time.Now())

	t.Run("due when the last run is long past", func(t *testing.T) {
		if !shouldRunNow(monthlyFirstOfMonth, &longAgo, longAgo) {
			t.Fatal("expected due after 90 days on a monthly schedule")
		}
	})

	t.Run("not due when it just ran", func(t *testing.T) {
		if shouldRunNow(monthlyFirstOfMonth, &justNow, longAgo) {
			t.Fatal("expected not due immediately after a run")
		}
	})

	// A zero LastRunTime is the sentinel handleInitialization writes to mark a resource
	// as initialised but never run. It must be treated as due, or a freshly created
	// WorkPackages would never fire.
	t.Run("zero last-run sentinel means due", func(t *testing.T) {
		zero := metav1.Time{}
		if !shouldRunNow(monthlyFirstOfMonth, &zero, justNow) {
			t.Fatal("expected the zero LastRunTime sentinel to be treated as due")
		}
	})

	t.Run("falls back to creation time when never run", func(t *testing.T) {
		if !shouldRunNow(monthlyFirstOfMonth, nil, longAgo) {
			t.Fatal("expected due when never run and created long ago")
		}
	})

	t.Run("invalid schedule never runs", func(t *testing.T) {
		if shouldRunNow("not-a-cron", &longAgo, longAgo) {
			t.Fatal("an unparseable schedule must not be treated as due")
		}
	})
}

// --------------------------------------------------------------------------------------
// extractID
// --------------------------------------------------------------------------------------

func TestFunctionalExtractID(t *testing.T) {
	cases := []struct {
		name string
		body string
		want string
	}{
		{"numeric id", `{"id":705,"subject":"September EKS Inventory"}`, "705"},
		{"string id", `{"id":"705"}`, "705"},
		{"no id field", `{"_type":"Error","message":"boom"}`, ""},
		{"malformed json", `{not json`, ""},
		{"empty body", ``, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := &http.Response{Body: io.NopCloser(strings.NewReader(tc.body))}
			if got := extractID(resp); got != tc.want {
				t.Fatalf("extractID = %q, want %q", got, tc.want)
			}
		})
	}
}

// --------------------------------------------------------------------------------------
// makeOpenProjectRequest
// --------------------------------------------------------------------------------------

// decodeBasic pulls the credentials back out of an Authorization: Basic header.
func decodeBasic(t *testing.T, header string) string {
	t.Helper()
	if !strings.HasPrefix(header, "Basic ") {
		t.Fatalf("expected a Basic auth header, got %q", header)
	}
	raw, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(header, "Basic "))
	if err != nil {
		t.Fatalf("Authorization header was not valid base64: %v", err)
	}
	return string(raw)
}

func TestFunctionalMakeOpenProjectRequestAuthAndMethod(t *testing.T) {
	var gotAuth, gotMethod, gotContentType string
	var gotBody []byte

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotContentType = r.Header.Get("Content-Type")
		gotMethod = r.Method
		gotBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"id":705}`))
	}))
	defer srv.Close()

	// 70 characters: not a multiple of 4, so it cannot be mistaken for base64.
	apiKey := strings.Repeat("a", 70)
	payload := []byte(`{"subject":"probe"}`)

	resp, err := makeOpenProjectRequest(context.Background(), http.MethodPost, srv.URL, apiKey, payload)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if gotMethod != http.MethodPost {
		t.Fatalf("method = %s, want POST", gotMethod)
	}
	if gotContentType != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", gotContentType)
	}
	if string(gotBody) != string(payload) {
		t.Fatalf("body = %q, want %q", gotBody, payload)
	}
	if want := "apikey:" + apiKey; decodeBasic(t, gotAuth) != want {
		t.Fatalf("auth credentials = %q, want %q", decodeBasic(t, gotAuth), want)
	}
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d, want 201", resp.StatusCode)
	}
	if id := extractID(resp); id != "705" {
		t.Fatalf("extractID = %q, want 705", id)
	}
}

// TestFunctionalBase64LookalikeKeyIsMangled documents a SECOND REAL DEFECT.
//
// makeOpenProjectRequest tries base64-decoding the API key first and, if that succeeds,
// sends the DECODED bytes as the credential. Plenty of ordinary tokens are accidentally
// valid base64 - any token whose length is a multiple of 4 drawn from the base64 alphabet
// qualifies - and those are silently corrupted before they are ever sent.
//
// This is the likely explanation for the stored 88-character token being rejected with
// 401 on 2026-09-02 while a 70-character replacement worked: 88 is a multiple of 4, 70 is
// not. The fix is to stop guessing at the encoding and send the key as given, but that
// changes behaviour for anyone who stored a genuinely base64-wrapped key, so it is
// recorded here rather than quietly changed.
//
// If the guessing is removed, THIS TEST SHOULD FAIL. Update it deliberately.
func TestFunctionalBase64LookalikeKeyIsMangled(t *testing.T) {
	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	// A 40-character token that is coincidentally valid base64.
	lookalike := strings.Repeat("QUJD", 10) // decodes to "ABC" x10
	if _, err := base64.StdEncoding.DecodeString(lookalike); err != nil {
		t.Fatalf("test fixture is not valid base64: %v", err)
	}

	resp, err := makeOpenProjectRequest(context.Background(), http.MethodGet, srv.URL, lookalike, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	sent := decodeBasic(t, gotAuth)
	asGiven := "apikey:" + lookalike
	if sent == asGiven {
		t.Fatalf("the key was sent unchanged (%q) - the base64 guessing has been removed, "+
			"which is the desired fix; update this test", sent)
	}
	if want := "apikey:" + strings.Repeat("ABC", 10); sent != want {
		t.Fatalf("expected the mangled credential %q, got %q", want, sent)
	}
}

// TestFunctionalNonSuccessStatusIsSurfaced covers the responses actually seen from
// OpenProject during the incident: 401 from an invalid key, 405 when posting to a path
// that does not accept the method, and 500 from the md5-on-FIPS database failure. The
// helper must return these to the caller rather than swallowing them, because the caller
// decides between "created" and "failed" on the status code.
func TestFunctionalNonSuccessStatusIsSurfaced(t *testing.T) {
	for _, code := range []int{
		http.StatusUnauthorized,        // 401 - invalid/mangled API key
		http.StatusMethodNotAllowed,    // 405 - wrong path for POST
		http.StatusInternalServerError, // 500 - md5() unavailable on FIPS postgres
	} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(code)
			_, _ = w.Write([]byte(`{"_type":"Error"}`))
		}))

		resp, err := makeOpenProjectRequest(context.Background(), http.MethodPost, srv.URL, "k", []byte(`{}`))
		if err != nil {
			srv.Close()
			t.Fatalf("status %d: unexpected transport error: %v", code, err)
		}
		if resp.StatusCode != code {
			_ = resp.Body.Close()
			srv.Close()
			t.Fatalf("status = %d, want %d", resp.StatusCode, code)
		}
		if id := extractID(resp); id != "" {
			_ = resp.Body.Close()
			srv.Close()
			t.Fatalf("expected no ticket id from a %d response, got %q", code, id)
		}
		_ = resp.Body.Close()
		srv.Close()
	}
}

func TestFunctionalRequestContextCancellation(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled

	if _, err := makeOpenProjectRequest(ctx, http.MethodGet, srv.URL, "k", nil); err == nil {
		t.Fatal("expected an error when the context is already cancelled")
	}
}
