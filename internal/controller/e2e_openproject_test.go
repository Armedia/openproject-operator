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

// TestE2EOpenProjectReconciliation is the real thing: a WorkPackages resource in a private
// API server, reconciled by the operator's own controller, must produce a ticket in the
// live OpenProject and record its id in status.
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
	if err := (&WorkPackageReconciler{Client: mgr.GetClient(), Scheme: mgr.GetScheme()}).SetupWithManager(mgr); err != nil {
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

	const ns = "default"
	stamp := time.Now().UTC().Format("20060102-150405")
	subject := fmt.Sprintf("%s %s", e2eSubjectPrefix, stamp)

	// Secret holding the API key, then the ServerConfig that points at the live server.
	if err := k8s.Create(ctx, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "e2e-openproject-api", Namespace: ns},
		StringData: map[string]string{"token": e.token},
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

	wp := &openprojectorgv1alpha1.WorkPackages{
		ObjectMeta: metav1.ObjectMeta{Name: "e2e-workpackage", Namespace: ns},
		Spec: openprojectorgv1alpha1.WorkPackagesSpec{
			Subject:         subject,
			Description:     "Created by the operator end-to-end verification. Safe to delete.",
			ProjectID:       e.projectID,
			TypeID:          e.typeID,
			// Every minute, NOT the monthly production shape, and that is deliberate.
			//
			// handleInitialization writes LastRunTime as a zero metav1.Time intending it as a
			// "never run, fire now" sentinel, and shouldRunNow honours it. But the field is
			// `omitempty`, so a zero time serialises to nothing and reads back as nil - the
			// sentinel does not survive a round-trip through the API server. shouldRunNow then
			// falls through to creationTime and waits for the next cron boundary.
			//
			// Verified here on 2026-09-03: with "0 0 1 * *" the resource sat at
			// status=Scheduled, nextRun=2026-10-01 and never fired. Production CRs are not
			// affected because they carry real LastRunTime values from previous runs; it is
			// newly created resources that never fire on creation.
			//
			// A minute schedule reaches its next boundary inside the test window, so this
			// exercises the real create path without depending on the broken sentinel.
			Schedule:        "* * * * *",
			ServerConfigRef: corev1.LocalObjectReference{Name: "e2e-serverconfig"},
		},
	}
	if err := k8s.Create(ctx, wp); err != nil {
		t.Fatalf("creating WorkPackages: %v", err)
	}

	// Whatever happens, do not leave a ticket behind.
	var ticketID string
	t.Cleanup(func() { e.deleteTicket(t, ticketID) })

	// The controller initialises first (writing a zero LastRunTime sentinel), then fires
	// on a subsequent reconcile. Poll status rather than guessing at the timing.
	deadline := time.Now().Add(3 * time.Minute)
	var last openprojectorgv1alpha1.WorkPackages
	for time.Now().Before(deadline) {
		if err := k8s.Get(ctx, types.NamespacedName{Name: "e2e-workpackage", Namespace: ns}, &last); err == nil {
			if last.Status.TicketID != "" {
				ticketID = last.Status.TicketID
				break
			}
			if last.Status.Status == StatusFailed {
				t.Fatalf("the controller reported failure: %q (see the OpenProject response above)", last.Status.Message)
			}
		}
		time.Sleep(2 * time.Second)
	}
	if ticketID == "" {
		t.Fatalf("no ticket was created within the deadline; last status=%q message=%q nextRun=%v",
			last.Status.Status, last.Status.Message, last.Status.NextRunTime)
	}
	t.Logf("controller reported ticket id %s", ticketID)

	// The status claiming success is not proof. Ask OpenProject.
	code, body := e.opRequest(t, http.MethodGet, "/api/v3/work_packages/"+ticketID)
	if code != http.StatusOK {
		t.Fatalf("ticket %s not retrievable from OpenProject (HTTP %d): %s", ticketID, code, truncate(body, 200))
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
	// The operator PREPENDS the period name to the configured subject, which is why the
	// production tickets read "September AWS Systems Inventory" while the CR only says
	// "AWS Systems Inventory". Assert the suffix so the test documents that behaviour
	// instead of pinning a month name that changes every cycle.
	if !strings.HasSuffix(got.Subject, subject) {
		t.Fatalf("ticket subject = %q, want it to end with %q", got.Subject, subject)
	}
	if got.Subject == subject {
		t.Logf("note: no period prefix was added this run (subject was used verbatim)")
	} else {
		t.Logf("operator prefixed the subject with %q",
			strings.TrimSuffix(got.Subject, subject))
	}
	if want := fmt.Sprintf("/api/v3/projects/%d", e.projectID); !strings.HasSuffix(got.Links.Project.Href, want) {
		t.Fatalf("ticket landed in %q, want a project href ending %q", got.Links.Project.Href, want)
	}
	t.Logf("VERIFIED end to end: ticket %s exists in OpenProject with subject %q in project %d",
		ticketID, got.Subject, e.projectID)

	// Status bookkeeping must also be correct, or the next cycle misbehaves.
	if last.Status.LastRunTime == nil || last.Status.LastRunTime.IsZero() {
		t.Error("LastRunTime was not set after a successful creation")
	}
	if last.Status.NextRunTime == nil || last.Status.NextRunTime.IsZero() {
		t.Error("NextRunTime was not set after a successful creation")
	}
}

func truncate(b []byte, n int) string {
	s := strings.TrimSpace(string(b))
	if len(s) > n {
		return s[:n] + "..."
	}
	return s
}

// TestE2EOpenProjectCloudInventory exercises the INVENTORY path, which is a different
// code path from the plain ticket above and is how the monthly AWS/EKS inventory tickets
// are produced in production.
//
// Note CloudInventoryReconciler.Reconcile is a no-op - it fetches the resource and
// returns. Inventories are not run on a schedule of their own; they are triggered by the
// WorkPackages controller via triggerCloudInventoryScan when a WorkPackages carries an
// InventoryRef. So this test drives it the way production does, through a WorkPackages.
//
// Kubernetes mode is used rather than AWS: it inventories the cluster the operator is
// pointed at, which here is the private envtest API server, so the test needs no cloud
// credentials and touches no real AWS account.
func TestE2EOpenProjectCloudInventory(t *testing.T) {
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
	if err := (&WorkPackageReconciler{Client: mgr.GetClient(), Scheme: mgr.GetScheme()}).SetupWithManager(mgr); err != nil {
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

	// reconcileKubernetes in LOCAL mode builds its client from ctrl.GetConfig(), which
	// reads the ambient kubeconfig or in-cluster service account - NOT the envtest config
	// the manager was handed. Without this the scan fails to build a client, returns an
	// error, and the ticket is still created with no report attached, which looks exactly
	// like "the inventory silently did not run".
	//
	// Minting an envtest user and exporting KUBECONFIG points ctrl.GetConfig() at the
	// private API server, so the inventory runs against envtest and touches nothing real.
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
	stamp := time.Now().UTC().Format("20060102-150405")
	subject := fmt.Sprintf("%s inventory %s", e2eSubjectPrefix, stamp)

	if err := k8s.Create(ctx, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "e2e-openproject-api", Namespace: ns},
		StringData: map[string]string{"token": e.token},
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

	// Inventory the envtest cluster itself. Empty namespaces means "all".
	if err := k8s.Create(ctx, &openprojectorgv1alpha1.CloudInventory{
		ObjectMeta: metav1.ObjectMeta{Name: "e2e-inventory", Namespace: ns},
		Spec: openprojectorgv1alpha1.CloudInventorySpec{
			Mode:       "kubernetes",
			Kubernetes: &openprojectorgv1alpha1.KubernetesInventorySpec{},
		},
	}); err != nil {
		t.Fatalf("creating CloudInventory: %v", err)
	}

	inventoryRef := corev1.LocalObjectReference{Name: "e2e-inventory"}
	if err := k8s.Create(ctx, &openprojectorgv1alpha1.WorkPackages{
		ObjectMeta: metav1.ObjectMeta{Name: "e2e-inventory-workpackage", Namespace: ns},
		Spec: openprojectorgv1alpha1.WorkPackagesSpec{
			Subject:         subject,
			Description:     "Inventory verification. Safe to delete.",
			ProjectID:       e.projectID,
			TypeID:          e.typeID,
			Schedule:        "* * * * *", // see the note on the sentinel in the test above
			ServerConfigRef: corev1.LocalObjectReference{Name: "e2e-serverconfig"},
			InventoryRef:    &inventoryRef,
		},
	}); err != nil {
		t.Fatalf("creating WorkPackages: %v", err)
	}

	var ticketID string
	t.Cleanup(func() { e.deleteTicket(t, ticketID) })

	deadline := time.Now().Add(3 * time.Minute)
	var last openprojectorgv1alpha1.WorkPackages
	for time.Now().Before(deadline) {
		if err := k8s.Get(ctx, types.NamespacedName{Name: "e2e-inventory-workpackage", Namespace: ns}, &last); err == nil {
			if last.Status.TicketID != "" {
				ticketID = last.Status.TicketID
				break
			}
			if last.Status.Status == StatusFailed {
				t.Fatalf("controller reported failure: %q", last.Status.Message)
			}
		}
		time.Sleep(2 * time.Second)
	}
	if ticketID == "" {
		t.Fatalf("no inventory ticket created; last status=%q message=%q", last.Status.Status, last.Status.Message)
	}

	// A CloudInventoryReport must have been produced as a side effect of the scan.
	var reports openprojectorgv1alpha1.CloudInventoryReportList
	if err := k8s.List(ctx, &reports, client.InNamespace(ns)); err != nil {
		t.Fatalf("listing CloudInventoryReports: %v", err)
	}
	if len(reports.Items) == 0 {
		t.Fatal("the ticket was created but no CloudInventoryReport was produced - the inventory did not run")
	}
	t.Logf("VERIFIED inventory path: ticket %s created and %d CloudInventoryReport(s) produced (summary=%v)",
		ticketID, len(reports.Items), reports.Items[0].Status.Summary)
}
