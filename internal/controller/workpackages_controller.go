package controller

import (
	"k8s.io/client-go/rest"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/go-logr/logr"
	"github.com/robfig/cron/v3"
	v1alpha1 "github.com/shrapk2/openproject-operator/api/v1alpha1"
	"github.com/shrapk2/openproject-operator/internal/configloader"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// Constants for status values
const (
	StatusScheduled = "Scheduled"
	StatusCreated   = "Created"
	StatusFailed    = "Failed"
)

// Constants for index field source reference names
const IndexFieldSourceRefName = "spec.sourceRef.name"

var (
	// Debug mode configuration
	// debugEnabled = os.Getenv("DEBUG") == "true"

	// Time configurations with environment variables and defaults
	DefaultRequeueTime = getDurationFromEnv("DEFAULT_REQUEUE_TIME", time.Minute*1)
	ShortRequeueTime   = getDurationFromEnv("SHORT_REQUEUE_TIME", time.Second*30)
	RequestTimeout     = getDurationFromEnv("REQUEST_TIMEOUT", time.Second*90)

	// Reusable HTTP client
	httpClient = &http.Client{Timeout: RequestTimeout}
)

// getDurationFromEnv reads a duration from an environment variable with a default fallback
func getDurationFromEnv(key string, defaultValue time.Duration) time.Duration {
	envValue := os.Getenv(key)
	if envValue == "" {
		return defaultValue
	}

	duration, err := time.ParseDuration(envValue)
	if err != nil {
		// Log error but use default value
		fmt.Printf("❌ Invalid duration format for %s: %s. Using default: %s\n",
			key, envValue, defaultValue)
		return defaultValue
	}

	return duration
}

// getScopedLogger returns a simplified logger for normal mode or a detailed logger for debug mode
func getScopedLogger(ctx context.Context, wp *v1alpha1.WorkPackages) logr.Logger {
	if debugEnabled {
		// In debug mode, use the full context logger from controller-runtime
		return log.FromContext(ctx)
	}

	// For normal mode, create a fresh logger with just the essential fields
	return ctrl.Log.WithValues("workpackage", wp.Name)
}

// statusLog logs a message in a structured way with status as a field
func statusLog(l logr.Logger, statusEmoji string, message string, keysAndValues ...interface{}) {
	// Create a status field with emoji and message
	status := fmt.Sprintf("%s %s", statusEmoji, message)

	// Prepare a slice with "status" as the first key-value pair
	kvs := append([]interface{}{"status", status}, keysAndValues...)

	// Log with empty message to make fields the primary content
	l.Info("", kvs...)
}

// WorkPackageStatusUpdate represents a status update operation
type WorkPackageStatusUpdate struct {
	LastRunTime *metav1.Time
	NextRunTime *metav1.Time
	TicketID    string
	Status      string
	Message     string
}

// WorkPackageReconciler reconciles a WorkPackages object
type WorkPackageReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	// Config is the manager's rest.Config, threaded through to CloudInventory scans so
	// local-mode inventory does not fall back to the ambient kubeconfig. Optional: when
	// nil the inventory keeps its previous ctrl.GetConfig() behaviour.
	Config *rest.Config
}

// maybeBase64 makes a best guess if a string is base64 encoded
// func maybeBase64(s string) bool {
// 	// Quick check for base64 patterns
// 	if len(s) == 0 || len(s)%4 != 0 {
// 		return false
// 	}

// 	// Check if it contains only valid base64 characters
// 	for _, c := range s {
// 		if !((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
// 			(c >= '0' && c <= '9') || c == '+' || c == '/' || c == '=') {
// 			return false
// 		}
// 	}

// 	return true
// }

// makeOpenProjectRequest creates and executes an OpenProject API request
func makeOpenProjectRequest(ctx context.Context, method, url, apiKey string, payload []byte) (*http.Response, error) {
	reqCtx, cancel := context.WithTimeout(ctx, RequestTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, method, url, bytes.NewBuffer(payload))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	//Try to directly decode the base64 key first
	decodedBytes, err := base64.StdEncoding.DecodeString(apiKey)
	if err == nil {
		// Successfully decoded - it was base64, use the decoded value without any prefix
		// The key appears to be already in the correct format
		rawKey := strings.TrimSpace(string(decodedBytes))
		// Re-encode it properly for the Authorization header
		authValue := base64.StdEncoding.EncodeToString([]byte("apikey:" + rawKey))
		req.Header.Set("Authorization", fmt.Sprintf("Basic %s", authValue))

		if debugEnabled {
			fmt.Printf("API Key was base64, decoded to: %s\n", rawKey)
			fmt.Printf("Auth header value: %s\n", authValue)
		}

	} else {
		// Not base64 or corrupt - use the raw key
		rawKey := strings.TrimSpace(apiKey)
		authValue := base64.StdEncoding.EncodeToString([]byte("apikey:" + rawKey))
		req.Header.Set("Authorization", fmt.Sprintf("Basic %s", authValue))

		if debugEnabled {
			fmt.Printf("API Key was NOT base64, using raw: %s\n", rawKey)
			fmt.Printf("Auth header value: %s\n", authValue)
		}
	}

	req.Header.Set("Content-Type", "application/json")

	return httpClient.Do(req)
}

// parseSchedule parses a cron schedule with proper error handling
func parseSchedule(schedule string) (cron.Schedule, error) {
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	return parser.Parse(schedule)
}

// calculateNextRunTime calculates the next run time based on a schedule and reference time
func calculateNextRunTime(schedule string, from time.Time) (time.Time, error) {
	spec, err := parseSchedule(schedule)
	if err != nil {
		return time.Time{}, err
	}
	return spec.Next(from), nil
}

// applyStatusUpdate applies a status update to a WorkPackages resource
func applyStatusUpdate(ctx context.Context, r *WorkPackageReconciler, wp *v1alpha1.WorkPackages,
	update WorkPackageStatusUpdate, log logr.Logger) error {
	original := wp.DeepCopy()

	if update.LastRunTime != nil {
		wp.Status.LastRunTime = update.LastRunTime
	}
	if update.NextRunTime != nil {
		wp.Status.NextRunTime = update.NextRunTime
	}
	if update.TicketID != "" {
		wp.Status.TicketID = update.TicketID
	}
	if update.Status != "" {
		wp.Status.Status = update.Status
	}
	if update.Message != "" {
		wp.Status.Message = update.Message
	}

	return r.Status().Patch(ctx, wp, client.MergeFrom(original))
}

// processAdditionalFields merges additional fields into the payload
func processAdditionalFields(payload map[string]interface{}, additionalFields v1alpha1.JSON) {
	// Convert the JSON to a map we can work with
	var fields map[string]interface{}
	if err := json.Unmarshal(additionalFields.Raw.Raw, &fields); err != nil {
		if debugEnabled {
			fmt.Printf("Error unmarshaling additional fields: %v\n", err)
		}
		return
	}

	// Process regular fields
	for key, value := range fields {
		// Skip _links which need special handling
		if key == "_links" {
			continue
		}
		payload[key] = value
	}

	// Process _links separately to merge with existing ones
	if linksRaw, ok := fields["_links"].(map[string]interface{}); ok {
		existingLinks, _ := payload["_links"].(map[string]interface{})
		for linkKey, linkValue := range linksRaw {
			existingLinks[linkKey] = linkValue
		}
	}

	if debugEnabled {
		jsonData, _ := json.MarshalIndent(payload, "", "  ")
		fmt.Printf("Final payload with additional fields:\n%s\n", string(jsonData))
	}
}

// buildTicketPayload constructs the payload for creating a ticket
func (r *WorkPackageReconciler) buildTicketPayload(
	ctx context.Context,
	wp *v1alpha1.WorkPackages,
	log logr.Logger,
) (map[string]interface{}, error) {
	var reportMarkdown string

	report, logs, err := r.triggerCloudInventoryScan(ctx, wp, log)
	if err != nil {
		log.Error(err, "❌ Cloud inventory scan failed")
		reportMarkdown = "_Cloud inventory scan failed._\n"
	} else if report != nil {
		reportMarkdown = BuildInventoryMarkdownReport(report)
	}

	fullDescription := wp.Spec.Description

	if reportMarkdown != "" {
		fullDescription += fmt.Sprintf("\n\n_Inventory Reference: `%s`_\n", wp.Spec.InventoryRef.Name) + reportMarkdown
	}

	// add logs if debug is enabled
	if debugEnabled {
		fmt.Printf("Debug logs: %v\n", logs)

		if len(logs) > 0 {
			reportMarkdown += "\n---\n#### Inventory Diagnostics\n```\n" + strings.Join(logs, "\n") + "\n```\n"
		}
	}

	payload := map[string]interface{}{
		// "subject": wp.Spec.Subject,
		"subject": fmt.Sprintf("%s %s", time.Now().Format("January"), wp.Spec.Subject),
		"description": map[string]string{
			"format": "markdown",
			"raw":    fullDescription,
		},
		"_links": map[string]interface{}{
			"project": map[string]string{
				"href": fmt.Sprintf("/api/v3/projects/%d", wp.Spec.ProjectID),
			},
			"type": map[string]string{
				"href": fmt.Sprintf("/api/v3/types/%d", wp.Spec.TypeID),
			},
		},
	}

	if wp.Spec.EpicID > 0 {
		payload["_links"].(map[string]interface{})["parent"] = map[string]string{
			"href": fmt.Sprintf("/api/v3/work_packages/%d", wp.Spec.EpicID),
		}
	}

	if len(wp.Spec.AdditionalFields.Raw.Raw) > 0 {
		processAdditionalFields(payload, wp.Spec.AdditionalFields)
	}

	return payload, nil
}

// // extractID extracts the ticket ID from an HTTP response
func extractID(resp *http.Response) string {
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return ""
	}

	var result map[string]interface{}
	if err := json.Unmarshal(body, &result); err != nil {
		return ""
	}

	if id, ok := result["id"]; ok {
		return fmt.Sprintf("%v", id)
	}
	return ""
}
func (r *WorkPackageReconciler) triggerCloudInventoryScan(ctx context.Context, wp *v1alpha1.WorkPackages, log logr.Logger) (*v1alpha1.CloudInventoryReport, []string, error) {
	if wp.Spec.InventoryRef == nil {
		return nil, nil, nil
	}

	// Fetch the CloudInventory resource
	var inv v1alpha1.CloudInventory
	var logs []string
	if err := r.Get(ctx, client.ObjectKey{Name: wp.Spec.InventoryRef.Name, Namespace: wp.Namespace}, &inv); err != nil {
		msg := "❌ Failed to fetch CloudInventory: " + err.Error()
		log.Error(err, msg)
		logs = append(logs, msg)
		return nil, logs, err
	}
	logs = append(logs, fmt.Sprintf("Found CloudInventory: %s (mode: %s)", inv.Name, inv.Spec.Mode))

	// Prepare the reconciler
	cloudRec := &CloudInventoryReconciler{Client: r.Client, Scheme: r.Scheme, Config: r.Config}
	var err error

	// Dispatch based on Mode or which spec is present
	switch inv.Spec.Mode {
	case "aws":
		_, err = cloudRec.reconcileAWS(ctx, &inv, log)
	case "kubernetes":
		_, err = cloudRec.reconcileKubernetes(ctx, &inv, log)
	default:
		if inv.Spec.AWS != nil {
			_, err = cloudRec.reconcileAWS(ctx, &inv, log)
		} else if inv.Spec.Kubernetes != nil {
			_, err = cloudRec.reconcileKubernetes(ctx, &inv, log)
		} else {
			// No inventory spec defined; skip silently
			logs = append(logs, "No AWS or Kubernetes inventory spec defined; skipping")
			return nil, logs, nil
		}
	}

	if err != nil {
		msg := "❌ Failed to run CloudInventory scan: " + err.Error()
		log.Error(err, msg)
		logs = append(logs, msg)
		return nil, logs, err
	}

	var latest *v1alpha1.CloudInventoryReport
	for i := 0; i < 12; i++ { // Wait up to 2 minutes total (12 * 10s)
		time.Sleep(10 * time.Second)

		report, err := r.loadLatestInventoryReport(ctx, wp, log)
		if err != nil {
			msg := "⚠ Failed to load inventory report: " + err.Error()
			log.Error(err, msg)
			logs = append(logs, msg)
			continue
		}

		if report != nil {
			// Kubernetes: check if the report is ready
			if inv.Spec.Mode == "kubernetes" || (inv.Spec.Mode == "" && inv.Spec.Kubernetes != nil) {
				log.Info("✅ Kubernetes inventory report ready",
					"name", report.Name,
					"pods", report.Status.Summary["pods"],
					"images", len(report.Status.ContainerImages),
				)
				return report, logs, nil
			}
			// AWS: check if any service has data
			hasData := false
			logParams := []interface{}{}

			for _, service := range monitoredServices {
				count := service.GetCount(report)
				logParams = append(logParams, service.LogName, count)
				if count > 0 {
					hasData = true
				}
			}

			if hasData {
				log.Info("✅ Found populated CloudInventoryReport", logParams...)
				latest = report
				break
			}
		}

		log.Info("⌛ Waiting for populated inventory report...")
	}

	if latest == nil {
		msg := "⚠ Inventory report found but had no data — fallback to last known or return empty"
		log.Info(msg)
		logs = append(logs, msg)
	}

	return latest, logs, nil
}

// shouldRunNow determines if it's time to create a ticket.
//
// `status` is the persisted Status.Status. It is needed because the previous
// "initialized, awaiting first run" signal did not survive a round-trip through the API
// server: handleInitialization wrote LastRunTime as a ZERO metav1.Time, but metav1.Time
// marshals a zero value to JSON null, and the field is `omitempty`, so it read back as
// nil rather than as a non-nil pointer to zero. The zero branch below was therefore
// unreachable in a real cluster, and a freshly created WorkPackages never fired on
// creation - it waited for the next cron boundary. Observed 2026-09-03: a resource with
// schedule "0 0 1 * *" sat at status=Scheduled, nextRun=2026-10-01 and never ran.
//
// StatusScheduled is written ONLY by handleInitialization (success sets Created, failure
// sets Failed), so it is an accurate and durable marker for the same intent.
func shouldRunNow(schedule string, lastRun *metav1.Time, creationTime metav1.Time, status string) bool {
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	spec, err := parser.Parse(schedule)
	if err != nil {
		return false
	}

	// Initialized and never run: fire on this reconcile. Covers both the durable status
	// marker and the legacy zero-time sentinel, so resources written by an older operator
	// still behave the same way.
	if (lastRun == nil && status == StatusScheduled) || (lastRun != nil && lastRun.IsZero()) {
		return true
	}

	now := time.Now()
	last := creationTime.Time
	if lastRun != nil && !lastRun.IsZero() {
		last = lastRun.Time
	}
	return now.After(spec.Next(last))
}

// handleInitialization initializes a WorkPackages resource
func (r *WorkPackageReconciler) handleInitialization(ctx context.Context, wp *v1alpha1.WorkPackages, log logr.Logger) (ctrl.Result, error) {
	now := time.Now()
	next, err := calculateNextRunTime(wp.Spec.Schedule, now)
	if err != nil {
		log.Error(err, "❌ Failed to parse cron schedule")
		return ctrl.Result{}, err
	}

	update := WorkPackageStatusUpdate{
		NextRunTime: &metav1.Time{Time: next},
		Status:      StatusScheduled,
		Message:     "Next run scheduled",
		// Set an empty LastRunTime to mark as initialized
		LastRunTime: &metav1.Time{Time: time.Time{}},
	}

	if err := applyStatusUpdate(ctx, r, wp, update, log); err != nil {
		log.Error(err, "❌ Failed to patch initial status")
		return ctrl.Result{}, err
	}

	statusLog(log, "✅", "Initial status set", "nextRunTime", next.Format(time.RFC3339))
	return ctrl.Result{RequeueAfter: DefaultRequeueTime}, nil
}

// handleCreateTicket creates a ticket in OpenProject
func (r *WorkPackageReconciler) handleCreateTicket(ctx context.Context, wp *v1alpha1.WorkPackages, config *v1alpha1.ServerConfig, apiKey string, log logr.Logger) (ctrl.Result, error) {
	statusLog(log, "🔄", "Creating new ticket", "subject", wp.Spec.Subject)

	// Build the payload
	payload, err := r.buildTicketPayload(ctx, wp, log)
	if err != nil {
		log.Error(err, "❌ Failed to build ticket payload")
		return ctrl.Result{}, err
	}
	if err != nil {
		log.Error(err, "❌ Failed to build ticket payload")
		return ctrl.Result{}, err
	}

	jsonData, err := json.Marshal(payload)
	if err != nil {
		log.Error(err, "❌ Failed to marshal JSON payload")
		return ctrl.Result{}, err
	}

	// Prepare API request
	url := fmt.Sprintf("%s/api/v3/work_packages", config.Spec.Server)

	if debugEnabled {
		statusLog(log, "🐞", "Request JSON payload", "json", string(jsonData))
		statusLog(log, "🐞", "POST URL", "url", url)
	}

	// Send request to OpenProject API
	resp, err := makeOpenProjectRequest(ctx, "POST", url, apiKey, jsonData)
	if err != nil {
		log.Error(err, "❌ Failed to send request")
		return ctrl.Result{}, err
	}
	defer resp.Body.Close()

	// Handle error responses
	if resp.StatusCode >= 300 {
		if debugEnabled {
			statusLog(log, "⚠", "Non-2xx status from OpenProject",
				"status", resp.StatusCode,
				"url", url,
				"apikey", apiKey)
		} else {
			statusLog(log, "⚠", "Non-2xx status from OpenProject", "status", resp.StatusCode)
		}
		r.updateFailedStatus(ctx, wp, log)
		return ctrl.Result{RequeueAfter: DefaultRequeueTime}, nil
	}

	// Process successful response
	id := extractID(resp)
	now := time.Now()
	next, _ := calculateNextRunTime(wp.Spec.Schedule, now)

	// Update status
	update := WorkPackageStatusUpdate{
		LastRunTime: &metav1.Time{Time: now},
		NextRunTime: &metav1.Time{Time: next},
		TicketID:    id,
		Status:      StatusCreated,
		Message:     "Ticket successfully created",
	}

	if err := applyStatusUpdate(ctx, r, wp, update, log); err != nil {
		log.Error(err, "❌ Failed to patch status")
	} else {
		statusLog(log, "✅", "Successfully created ticket",
			"ticketID", id,
			"nextRunTime", next.Format(time.RFC3339))
	}

	return ctrl.Result{RequeueAfter: DefaultRequeueTime}, nil
}

// updateFailedStatus updates the status to reflect a failed ticket creation
func (r *WorkPackageReconciler) updateFailedStatus(ctx context.Context, wp *v1alpha1.WorkPackages, log logr.Logger) {
	now := time.Now()
	next, err := calculateNextRunTime(wp.Spec.Schedule, now)
	if err != nil {
		log.Error(err, "❌ Failed to calculate next run time")
		return
	}

	update := WorkPackageStatusUpdate{
		NextRunTime: &metav1.Time{Time: next},
		Status:      StatusFailed,
		Message:     "Ticket creation failed",
	}

	if err := applyStatusUpdate(ctx, r, wp, update, log); err != nil {
		log.Error(err, "❌ Failed to update failed status")
	}

	statusLog(log, "❌", "Ticket creation failed", "nextRetry", next.Format(time.RFC3339))
}

// loadConfig loads the server configuration and API key
func (r *WorkPackageReconciler) loadConfig(ctx context.Context, wp *v1alpha1.WorkPackages, log logr.Logger) (*v1alpha1.ServerConfig, string, error) {
	config, err := configloader.LoadServerConfig(ctx, r.Client, wp.Spec.ServerConfigRef.Name, wp.Namespace)
	if err != nil {
		log.Error(err, "❌ Could not load ServerConfig", "serverconfig", wp.Spec.ServerConfigRef.Name)
		return nil, "", err
	}
	statusLog(log, "🛠", "ServerConfig loaded", "serverconfig", wp.Spec.ServerConfigRef.Name)

	apiKey, err := configloader.LoadAPIKey(ctx, r.Client, config)
	if err != nil {
		log.Error(err, "❌ Failed to load OpenProject API key", "serverconfig", config.Name)
		return nil, "", err
	}

	return config, strings.TrimSpace(apiKey), nil
}

// +kubebuilder:rbac:groups=openproject.org,resources=workpackages,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=openproject.org,resources=workpackages/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=openproject.org,resources=workpackages/finalizers,verbs=update
func (r *WorkPackageReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	// Get the WorkPackages resource
	var wp v1alpha1.WorkPackages
	if err := r.Get(ctx, req.NamespacedName, &wp); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	log := getScopedLogger(ctx, &wp)

	// Initialize if needed
	if wp.Status.LastRunTime == nil {
		if wp.Status.Status != StatusScheduled || wp.Status.NextRunTime == nil {
			return r.handleInitialization(ctx, &wp, log)
		}
	}

	// Check if it's time to run
	if !shouldRunNow(wp.Spec.Schedule, wp.Status.LastRunTime, wp.CreationTimestamp, wp.Status.Status) {
		statusLog(log, "⏳", "Not time to run yet based on schedule", "schedule", wp.Spec.Schedule)
		return ctrl.Result{RequeueAfter: DefaultRequeueTime}, nil
	}

	// Load configuration
	config, apiKey, err := r.loadConfig(ctx, &wp, log)
	if err != nil {
		return ctrl.Result{}, err
	}

	// Create the ticket
	return r.handleCreateTicket(ctx, &wp, config, apiKey, log)
}

// SetupWithManager sets up the controller with the Manager.
func (r *WorkPackageReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if err := mgr.GetFieldIndexer().IndexField(context.TODO(), &v1alpha1.CloudInventoryReport{},
		IndexFieldSourceRefName, func(obj client.Object) []string {
			report := obj.(*v1alpha1.CloudInventoryReport)
			return []string{report.Spec.SourceRef.Name}
		}); err != nil {
		return err
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.WorkPackages{}).
		Complete(r)
}

func (r *WorkPackageReconciler) loadLatestInventoryReport(ctx context.Context, wp *v1alpha1.WorkPackages, log logr.Logger) (*v1alpha1.CloudInventoryReport, error) {
	if wp.Spec.InventoryRef == nil {
		return nil, nil
	}

	var reports v1alpha1.CloudInventoryReportList
	if err := r.List(ctx, &reports, client.InNamespace(wp.Namespace),
		client.MatchingFields{IndexFieldSourceRefName: wp.Spec.InventoryRef.Name}); err != nil {
		log.Error(err, "❌ Failed to list inventory reports", "name", wp.Spec.InventoryRef.Name)
		return nil, err
	}

	if len(reports.Items) == 0 {
		log.Info("⚠ No inventory reports found", "inventory", wp.Spec.InventoryRef.Name)
		return nil, nil
	}

	// Pick latest by timestamp
	latest := reports.Items[0]
	for _, r := range reports.Items {
		if r.Spec.Timestamp.After(latest.Spec.Timestamp.Time) {
			latest = r
		}
	}

	// Determine mode by fetching the CloudInventory CR
	var inv v1alpha1.CloudInventory
	if err := r.Get(ctx, client.ObjectKey{Name: wp.Spec.InventoryRef.Name, Namespace: wp.Namespace}, &inv); err == nil {
		isK8s := inv.Spec.Mode == "kubernetes" || inv.Spec.Kubernetes != nil
		age := time.Since(latest.Spec.Timestamp.Time)
		if isK8s {
			// Simple log for Kubernetes
			log.Info("Found Kubernetes inventory report", "name", latest.Name, "age", age)
			return &latest, nil
		}
	}

	// Continue with AWS-specific logging
	logParams := []interface{}{
		"name", latest.Name,
		"age", time.Since(latest.Spec.Timestamp.Time),
	}

	for _, service := range monitoredServices {
		logParams = append(logParams, service.LogName, service.GetCount(&latest))
	}

	log.Info("Found inventory report", logParams...)

	// If status is empty but we expect data to be there
	hasData := false
	for _, service := range monitoredServices {
		if service.GetCount(&latest) > 0 {
			hasData = true
			break
		}
	}

	if !hasData {
		// Try to get the fresh version directly
		var freshReport v1alpha1.CloudInventoryReport
		if err := r.Get(ctx, client.ObjectKey{Name: latest.Name, Namespace: latest.Namespace}, &freshReport); err != nil {
			log.Error(err, "❌ Failed to get latest report directly", "name", latest.Name)
		} else {
			// Check fresh report for data
			freshLogParams := []interface{}{
				"name", freshReport.Name,
			}

			freshHasData := false
			for _, service := range monitoredServices {
				count := service.GetCount(&freshReport)
				freshLogParams = append(freshLogParams, service.LogName, count)
				if count > 0 {
					freshHasData = true
				}
			}

			log.Info("Refreshed report data", freshLogParams...)

			if freshHasData {
				return &freshReport, nil
			}
		}
	}

	return &latest, nil
}
