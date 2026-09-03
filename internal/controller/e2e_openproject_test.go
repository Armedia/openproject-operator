package controller

// End-to-end verification against a LIVE OpenProject.
//
// WHY THIS EXISTS
// ---------------
// The unit tests in workpackages_functional_test.go prove the pure logic and the HTTP
// shape against an httptest stub. They cannot prove the operator actually creates a
// ticket in a real OpenProject, which is the thing that produces FedRAMP compliance
// evidence. This test closes that gap so a rebuilt image can be validated BEFORE it
// replaces the running operator.
//
// WHY envtest AND NOT A SCRATCH NAMESPACE
// ---------------------------------------
// The operator has NO namespace scoping - no --namespace flag, no WATCH_NAMESPACE, and
// the manager is built with an unrestricted cache. A second instance deployed anywhere in
// the production cluster would therefore reconcile the PRODUCTION WorkPackages and
// CloudInventory resources as well as the test ones: duplicate tickets, two controllers
// writing the same status. That is strictly worse than not testing.
//
// envtest gives a real kube-apiserver and etcd, private to this process, containing only
// the resources created here - while the OpenProject calls go to the real server. Full
// isolation on the Kubernetes side, genuine end-to-end on the OpenProject side.
//
// If namespace scoping is ever added to the manager, an in-cluster scratch namespace
// becomes viable and this note should be revisited.
//
// HOW TO RUN
// ----------
//	export E2E_OPENPROJECT_URL=https://project.armedia.us
//	export E2E_OPENPROJECT_TOKEN=<an OpenProject API key>
//	export E2E_PROJECT_ID=1          # optional, defaults to 1 (Demo project)
//	export E2E_TYPE_ID=1             # optional, defaults to 1 (Task)
//	export KUBEBUILDER_ASSETS="$(setup-envtest use 1.31.0 -p path)"
//	go test ./internal/controller/ -run TestE2EOpenProject -v -timeout 10m
//
// It SKIPS when the URL/token are absent, so `go test ./...` stays green without
// credentials and in CI.
//
// SAFETY
// ------
//   - Defaults to project 1 (Demo), never the FedRAMP project. Point E2E_PROJECT_ID at a
//     scratch project; do not aim this at project 4.
//   - Every ticket it creates is deleted in a t.Cleanup, including on failure.
//   - Subjects are timestamped and prefixed so anything left behind is identifiable.

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"path/filepath"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	openprojectorgv1alpha1 "github.com/shrapk2/openproject-operator/api/v1alpha1"
)

const e2eSubjectPrefix = "e2e-operator-verification"

type e2eEnv struct {
	url, token string
	projectID  int
	typeID     int
}

func e2eConfig(t *testing.T) e2eEnv {
	t.Helper()
	url := strings.TrimRight(os.Getenv("E2E_OPENPROJECT_URL"), "/")
	token := os.Getenv("E2E_OPENPROJECT_TOKEN")
	if url == "" || token == "" {
		t.Skip("E2E_OPENPROJECT_URL and E2E_OPENPROJECT_TOKEN not set - skipping live verification")
	}
	atoiOr := func(key string, def int) int {
		if v := os.Getenv(key); v != "" {
			if n, err := strconv.Atoi(v); err == nil {
				return n
			}
		}
		return def
	}
	return e2eEnv{url: url, token: token,
		projectID: atoiOr("E2E_PROJECT_ID", 1), typeID: atoiOr("E2E_TYPE_ID", 1)}
}

// opGet performs an authenticated GET against OpenProject and returns status and body.
func (e e2eEnv) opRequest(t *testing.T, method, path string) (int, []byte) {
	t.Helper()
	req, err := http.NewRequest(method, e.url+path, nil)
	if err != nil {
		t.Fatalf("building request: %v", err)
	}
	req.SetBasicAuth("apikey", e.token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, body
}

// deleteTicket removes a work package. Used from cleanup, so it reports rather than fails.
func (e e2eEnv) deleteTicket(t *testing.T, id string) {
	t.Helper()
	if id == "" {
		return
	}
	code, _ := e.opRequest(t, http.MethodDelete, "/api/v3/work_packages/"+id)
	if code != http.StatusNoContent && code != http.StatusNotFound {
		t.Logf("WARNING: could not delete ticket %s (HTTP %d) - remove it by hand", id, code)
		return
	}
	t.Logf("cleaned up ticket %s", id)
}


// createTicket makes a work package directly, so a test can plant one for the operator to
// find. Returns the new id.
func (e e2eEnv) createTicket(t *testing.T, subject string) string {
	t.Helper()
	body := fmt.Sprintf(
		`{"subject":%s,"_links":{"type":{"href":"/api/v3/types/%d"},"project":{"href":"/api/v3/projects/%d"}}}`,
		strconvQuote(subject), e.typeID, e.projectID)
	req, err := http.NewRequest(http.MethodPost, e.url+"/api/v3/work_packages", strings.NewReader(body))
	if err != nil {
		t.Fatalf("building create request: %v", err)
	}
	req.SetBasicAuth("apikey", e.token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("creating ticket: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		t.Fatalf("creating ticket returned HTTP %d: %s", resp.StatusCode, truncate(raw, 200))
	}
	var got struct {
		ID int `json:"id"`
	}
	if err := json.Unmarshal(raw, &got); err != nil || got.ID == 0 {
		t.Fatalf("could not read new ticket id from %s", truncate(raw, 200))
	}
	return strconv.Itoa(got.ID)
}

func strconvQuote(s string) string { return strconv.Quote(s) }

// TestE2EOpenProjectPreflight fails fast with a clear reason when the environment is not
// usable, so a later timeout is never mistaken for an operator defect.
func TestE2EOpenProjectPreflight(t *testing.T) {
	e := e2eConfig(t)

	if code, body := e.opRequest(t, http.MethodGet, "/api/v3/users/me"); code != http.StatusOK {
		t.Fatalf("authentication failed (HTTP %d). The token is invalid, or it is a "+
			"base64-lookalike being mangled - see TestFunctionalBase64LookalikeKeyIsMangled. Body: %s",
			code, truncate(body, 200))
	}
	if code, _ := e.opRequest(t, http.MethodGet, fmt.Sprintf("/api/v3/projects/%d", e.projectID)); code != http.StatusOK {
		t.Fatalf("project %d is not reachable (HTTP %d)", e.projectID, code)
	}
	// The work_packages collection is the endpoint that fails when the database cannot
	// compute md5() - the FIPS PostgreSQL failure of 2026-09-02. Check it explicitly so
	// that condition is named rather than surfacing as a mysterious creation failure.
	if code, body := e.opRequest(t, http.MethodGet, "/api/v3/work_packages?pageSize=1"); code != http.StatusOK {
		t.Fatalf("the work_packages API is unhealthy (HTTP %d). If this is 500, check the "+
			"database for 'could not compute MD5 hash: unsupported' - work-package queries "+
			"build an MD5 cache key that FIPS PostgreSQL cannot execute. Body: %s",
			code, truncate(body, 200))
	}
	if e.projectID == 4 {
		t.Fatalf("refusing to run against project 4 (FedRAMP); point E2E_PROJECT_ID at a scratch project")
	}
	t.Log("preflight OK: auth, project and work_packages API all healthy")
}

// TestE2EOpenProjectReconciliation drives both live paths against one envtest instance.
//
// The two scenarios share a single manager deliberately: controller-runtime registers
// controllers in a PROCESS-GLOBAL metrics registry, so wiring the same reconciler into a
// second manager in the same test binary fails with "controller with name workpackages
// already exists". Sharing is also faster - envtest start-up dominates the runtime.
func TestE2EOpenProjectReconciliation(t *testing.T) {
	e := e2eConfig(t)
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS not set - run `setup-envtest use 1.31.0 -p path` first")
	}

	testEnv := &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("..", "..", "config", "crd", "bases")},
		ErrorIfCRDPathMissing: true,
	}
	cfg, err := testEnv.Start()
	if err != nil {
		t.Fatalf("starting envtest: %v", err)
	}
	t.Cleanup(func() { _ = testEnv.Stop() })

	if err := openprojectorgv1alpha1.AddToScheme(scheme.Scheme); err != nil {
		t.Fatalf("adding scheme: %v", err)
	}
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{Scheme: scheme.Scheme})
	if err != nil {
		t.Fatalf("creating manager: %v", err)
	}
	if err := (&WorkPackageReconciler{
		Client: mgr.GetClient(), Scheme: mgr.GetScheme(), Config: mgr.GetConfig(),
	}).SetupWithManager(mgr); err != nil {
		t.Fatalf("wiring WorkPackageReconciler: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() {
		if err := mgr.Start(ctx); err != nil {
			t.Logf("manager stopped: %v", err)
		}
	}()
	if !mgr.GetCache().WaitForCacheSync(ctx) {
		t.Fatal("cache did not sync")
	}
	k8s := mgr.GetClient()

	// reconcileKubernetes in LOCAL mode builds its client from r.Config, falling back to
	// ctrl.GetConfig() - the ambient kubeconfig or in-cluster service account. Config is
	// now passed above, but export KUBECONFIG too so the fallback path is also correct if
	// this test is ever run against an operator build that predates that fix.
	adminUser, err := testEnv.AddUser(envtest.User{Name: "e2e-admin", Groups: []string{"system:masters"}}, nil)
	if err != nil {
		t.Fatalf("creating envtest user: %v", err)
	}
	kubeconfig, err := adminUser.KubeConfig()
	if err != nil {
		t.Fatalf("generating kubeconfig: %v", err)
	}
	kubeconfigPath := filepath.Join(t.TempDir(), "envtest.kubeconfig")
	if err := os.WriteFile(kubeconfigPath, kubeconfig, 0o600); err != nil {
		t.Fatalf("writing kubeconfig: %v", err)
	}
	t.Setenv("KUBECONFIG", kubeconfigPath)

	const ns = "default"

	// Shared credentials and server config for both scenarios.
	//
	// THE SECRET MUST HOLD THE BASE64-WRAPPED KEY, not the raw one. makeOpenProjectRequest
	// base64-decodes whatever it is given, so production stores the key wrapped and the
	// operator unwraps it. Handing it a RAW key that happens to be valid base64 - which a
	// 64-character hex OpenProject key always is, being a multiple of 4 from the base64
	// alphabet - gets it decoded into garbage and every call returns 401.
	//
	// That is not hypothetical: this test failed exactly that way on 2026-09-03 when the
	// raw armadmin key was passed straight through. It had previously passed only because
	// the token in use then was 70 characters, which is not a multiple of 4 and so fell to
	// the raw path. E2E_OPENPROJECT_TOKEN is the RAW key, used directly for this test's own
	// verification calls, and wrapped here for the operator.
	wrapped := base64.StdEncoding.EncodeToString([]byte(e.token))
	if err := k8s.Create(ctx, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "e2e-openproject-api", Namespace: ns},
		StringData: map[string]string{"token": wrapped},
	}); err != nil {
		t.Fatalf("creating secret: %v", err)
	}
	if err := k8s.Create(ctx, &openprojectorgv1alpha1.ServerConfig{
		ObjectMeta: metav1.ObjectMeta{Name: "e2e-serverconfig", Namespace: ns},
		Spec: openprojectorgv1alpha1.ServerConfigSpec{
			Server: e.url,
			APIKeySecretRef: corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: "e2e-openproject-api"},
				Key:                  "token",
			},
		},
	}); err != nil {
		t.Fatalf("creating ServerConfig: %v", err)
	}

	// waitForTicket polls a WorkPackages until it records a ticket id, and registers the
	// cleanup that deletes the ticket even when the test fails.
	waitForTicket := func(t *testing.T, name string) (string, openprojectorgv1alpha1.WorkPackages) {
		t.Helper()
		var ticketID string
		t.Cleanup(func() { e.deleteTicket(t, ticketID) })
		deadline := time.Now().Add(5 * time.Minute)
		var last openprojectorgv1alpha1.WorkPackages
		for time.Now().Before(deadline) {
			if err := k8s.Get(ctx, types.NamespacedName{Name: name, Namespace: ns}, &last); err == nil {
				if last.Status.TicketID != "" {
					// Assign before returning: the cleanup closure above captures this
					// variable, so returning the field directly would leave it empty and
					// the deletion would silently do nothing. That happened - it left
					// four tickets behind before being caught by checking the database.
					ticketID = last.Status.TicketID
					return ticketID, last
				}
				if last.Status.Status == StatusFailed {
					t.Fatalf("controller reported failure: %q", last.Status.Message)
				}
			}
			time.Sleep(2 * time.Second)
		}
		t.Fatalf("no ticket created within the deadline; last status=%q message=%q nextRun=%v",
			last.Status.Status, last.Status.Message, last.Status.NextRunTime)
		return "", last
	}

	t.Run("creates a ticket in OpenProject", func(t *testing.T) {
		stamp := time.Now().UTC().Format("20060102-150405")
		subject := fmt.Sprintf("%s %s", e2eSubjectPrefix, stamp)

		if err := k8s.Create(ctx, &openprojectorgv1alpha1.WorkPackages{
			ObjectMeta: metav1.ObjectMeta{Name: "e2e-workpackage", Namespace: ns},
			Spec: openprojectorgv1alpha1.WorkPackagesSpec{
				Subject:     subject,
				Description: "Created by the operator end-to-end verification. Safe to delete.",
				ProjectID:   e.projectID,
				TypeID:      e.typeID,
				// The MONTHLY production shape, and that is the point. Before the
				// shouldRunNow fix a new resource with this schedule sat at
				// status=Scheduled, nextRun=1 October and never fired, because the
				// zero-time sentinel did not survive serialisation. It must now fire on
				// the reconcile immediately after initialization.
				Schedule:        "0 0 1 * *",
				ServerConfigRef: corev1.LocalObjectReference{Name: "e2e-serverconfig"},
			},
		}); err != nil {
			t.Fatalf("creating WorkPackages: %v", err)
		}

		ticketID, last := waitForTicket(t, "e2e-workpackage")
		t.Logf("controller reported ticket id %s", ticketID)

		// The status claiming success is not proof. Ask OpenProject.
		code, body := e.opRequest(t, http.MethodGet, "/api/v3/work_packages/"+ticketID)
		if code != http.StatusOK {
			t.Fatalf("ticket %s not retrievable (HTTP %d): %s", ticketID, code, truncate(body, 200))
		}
		var got struct {
			Subject string `json:"subject"`
			Links   struct {
				Project struct {
					Href string `json:"href"`
				} `json:"project"`
			} `json:"_links"`
		}
		if err := json.Unmarshal(body, &got); err != nil {
			t.Fatalf("decoding ticket: %v", err)
		}
		// The operator PREPENDS the period name, which is why production tickets read
		// "September AWS Systems Inventory" while the CR says only the latter.
		if !strings.HasSuffix(got.Subject, subject) {
			t.Fatalf("ticket subject = %q, want it to end with %q", got.Subject, subject)
		}
		if want := fmt.Sprintf("/api/v3/projects/%d", e.projectID); !strings.HasSuffix(got.Links.Project.Href, want) {
			t.Fatalf("ticket landed in %q, want a project href ending %q", got.Links.Project.Href, want)
		}
		if last.Status.LastRunTime == nil || last.Status.LastRunTime.IsZero() {
			t.Error("LastRunTime was not set after a successful creation")
		}
		if last.Status.NextRunTime == nil || last.Status.NextRunTime.IsZero() {
			t.Error("NextRunTime was not set after a successful creation")
		}
		t.Logf("VERIFIED: ticket %s exists in OpenProject as %q in project %d",
			ticketID, got.Subject, e.projectID)
	})

	t.Run("adopts an existing ticket instead of duplicating", func(t *testing.T) {
		// The idempotency guard. Plant a ticket carrying the EXACT subject the operator
		// would compose - it prefixes the month name - then let the operator run. It must
		// find and adopt that ticket rather than creating a second one.
		//
		// This is the failure that produced 112 work packages: when the database could not
		// compute md5(), OpenProject COMMITTED each ticket and then failed while rendering
		// the response, so every retry added another.
		stamp := time.Now().UTC().Format("20060102-150405")
		specSubject := fmt.Sprintf("%s adopt %s", e2eSubjectPrefix, stamp)
		composed := fmt.Sprintf("%s %s", time.Now().Format("January"), specSubject)

		planted := e.createTicket(t, composed)
		t.Cleanup(func() { e.deleteTicket(t, planted) })
		t.Logf("planted ticket %s with subject %q", planted, composed)

		if err := k8s.Create(ctx, &openprojectorgv1alpha1.WorkPackages{
			ObjectMeta: metav1.ObjectMeta{Name: "e2e-adopt-workpackage", Namespace: ns},
			Spec: openprojectorgv1alpha1.WorkPackagesSpec{
				Subject:         specSubject,
				Description:     "Idempotency verification. Safe to delete.",
				ProjectID:       e.projectID,
				TypeID:          e.typeID,
				Schedule:        "0 0 1 * *",
				ServerConfigRef: corev1.LocalObjectReference{Name: "e2e-serverconfig"},
			},
		}); err != nil {
			t.Fatalf("creating WorkPackages: %v", err)
		}

		adoptedID, last := waitForTicket(t, "e2e-adopt-workpackage")

		if adoptedID != planted {
			// A different id means it created a second ticket - the guard did not work.
			t.Cleanup(func() { e.deleteTicket(t, adoptedID) })
			t.Fatalf("operator reported ticket %s but should have adopted the planted %s - "+
				"a duplicate was created", adoptedID, planted)
		}
		if last.Status.Status != StatusCreated {
			t.Errorf("status = %q, want %q after adoption", last.Status.Status, StatusCreated)
		}
		t.Logf("VERIFIED idempotency: adopted planted ticket %s, no duplicate created", adoptedID)
	})

	t.Run("runs an inventory and produces a report", func(t *testing.T) {
		// CloudInventoryReconciler.Reconcile is a no-op; inventories only run when a
		// WorkPackages triggers one via InventoryRef. Drive it the way production does.
		// Kubernetes mode inventories the envtest cluster, so no cloud credentials are
		// needed and no real AWS account is touched.
		if err := k8s.Create(ctx, &openprojectorgv1alpha1.CloudInventory{
			ObjectMeta: metav1.ObjectMeta{Name: "e2e-inventory", Namespace: ns},
			Spec: openprojectorgv1alpha1.CloudInventorySpec{
				Mode:       "kubernetes",
				Kubernetes: &openprojectorgv1alpha1.KubernetesInventorySpec{},
			},
		}); err != nil {
			t.Fatalf("creating CloudInventory: %v", err)
		}

		stamp := time.Now().UTC().Format("20060102-150405")
		inventoryRef := corev1.LocalObjectReference{Name: "e2e-inventory"}
		if err := k8s.Create(ctx, &openprojectorgv1alpha1.WorkPackages{
			ObjectMeta: metav1.ObjectMeta{Name: "e2e-inventory-workpackage", Namespace: ns},
			Spec: openprojectorgv1alpha1.WorkPackagesSpec{
				Subject:         fmt.Sprintf("%s inventory %s", e2eSubjectPrefix, stamp),
				Description:     "Inventory verification. Safe to delete.",
				ProjectID:       e.projectID,
				TypeID:          e.typeID,
				Schedule:        "0 0 1 * *",
				ServerConfigRef: corev1.LocalObjectReference{Name: "e2e-serverconfig"},
				InventoryRef:    &inventoryRef,
			},
		}); err != nil {
			t.Fatalf("creating WorkPackages: %v", err)
		}

		ticketID, _ := waitForTicket(t, "e2e-inventory-workpackage")

		var reports openprojectorgv1alpha1.CloudInventoryReportList
		if err := k8s.List(ctx, &reports, client.InNamespace(ns)); err != nil {
			t.Fatalf("listing CloudInventoryReports: %v", err)
		}
		if len(reports.Items) == 0 {
			t.Fatal("ticket created but no CloudInventoryReport was produced - the inventory did not run")
		}
		t.Logf("VERIFIED inventory: ticket %s created and %d report(s) produced (summary=%v)",
			ticketID, len(reports.Items), reports.Items[0].Status.Summary)
	})
}

func truncate(b []byte, n int) string {
	s := strings.TrimSpace(string(b))
	if len(s) > n {
		return s[:n] + "..."
	}
	return s
}
