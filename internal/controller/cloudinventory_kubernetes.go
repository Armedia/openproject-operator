package controller

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/shrapk2/openproject-operator/api/v1alpha1"
)

func (r *CloudInventoryReconciler) reconcileKubernetes(ctx context.Context, ci *v1alpha1.CloudInventory, log logr.Logger) (ctrl.Result, error) {
	original := ci.DeepCopy()

	// Skip rapid retries on failure
	if ci.Status.LastFailedTime != nil &&
		time.Since(ci.Status.LastFailedTime.Time) < 30*time.Second &&
		!ci.Status.LastRunSuccess {
		log.Info("🛑 Skipping Kubernetes inventory due to recent failure")
		return ctrl.Result{RequeueAfter: 10 * time.Minute}, nil
	}

	// Build REST config (local or remote)
	var restConfig *rest.Config
	var err error
	var clusterName string

	if ci.Spec.Kubernetes.KubeconfigSecretRef != nil {
		// Remote cluster
		restConfig, clusterName, err = r.buildRemoteKubeConfig(ctx, ci, log)
		if err != nil {
			ci.Status.LastFailedTime = &metav1.Time{Time: time.Now()}
			_ = r.Status().Patch(ctx, ci, client.MergeFrom(original))
			return ctrl.Result{}, err
		}
	} else {
		// Local cluster.
		//
		// Prefer the config the manager was built with. ctrl.GetConfig() reads the
		// AMBIENT kubeconfig or in-cluster service account, which is not necessarily the
		// cluster this operator is managing - and when neither is present it fails, the
		// scan returns an error, and the calling WorkPackages still creates its ticket
		// with no report attached. That failure reads as "the inventory silently did not
		// run". Falling back keeps behaviour unchanged when Config is not supplied.
		if r.Config != nil {
			restConfig = r.Config
		} else {
			restConfig, err = ctrl.GetConfig()
		}
		clusterName = "operator-local"
		if err != nil {
			ci.Status.LastFailedTime = &metav1.Time{Time: time.Now()}
			_ = r.Status().Patch(ctx, ci, client.MergeFrom(original))
			return ctrl.Result{}, err
		}
	}

	var clientset *kubernetes.Clientset
	clientset, err = kubernetes.NewForConfig(restConfig)
	if err != nil {
		log.Error(err, "failed to create Kubernetes client")
		ci.Status.LastFailedTime = &metav1.Time{Time: time.Now()}
		_ = r.Status().Patch(ctx, ci, client.MergeFrom(original))
		return ctrl.Result{}, err
	}

	// List pods (all namespaces or scoped)
	ns := ""
	if len(ci.Spec.Kubernetes.Namespaces) == 1 {
		ns = ci.Spec.Kubernetes.Namespaces[0]
	}

	podList, err := clientset.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{})
	if err != nil {
		log.Error(err, "failed to list pods")
		ci.Status.LastFailedTime = &metav1.Time{Time: time.Now()}
		_ = r.Status().Patch(ctx, ci, client.MergeFrom(original))
		return ctrl.Result{}, err
	}

	// Deduplicate images
	imageMap := make(map[string]v1alpha1.ContainerImageInfo)
	for _, pod := range podList.Items {
		statusMap := map[string]string{}
		for _, cs := range pod.Status.ContainerStatuses {
			statusMap[cs.Name] = cs.ImageID
		}
		for _, ctr := range pod.Spec.Containers {
			parsed := parseContainerImageWithDigest(ctr.Image, statusMap[ctr.Name])
			parsed.Cluster = clusterName
			if _, seen := imageMap[parsed.Image]; !seen {
				imageMap[parsed.Image] = parsed
			}
		}
	}
	images := make([]v1alpha1.ContainerImageInfo, 0, len(imageMap))
	for _, info := range imageMap {
		images = append(images, info)
	}
	for _, val := range imageMap {
		images = append(images, val)
	}

	// Build summary
	summary := map[string]int{
		"pods":   len(podList.Items),
		"images": len(images),
	}

	// Check for a recent report (age < 2m)
	var reports v1alpha1.CloudInventoryReportList
	if err := r.List(ctx, &reports, client.InNamespace(ci.Namespace)); err == nil {
		var latest *v1alpha1.CloudInventoryReport
		for _, rep := range reports.Items {
			if rep.Spec.SourceRef.Name == ci.Name {
				if latest == nil || rep.CreationTimestamp.After(latest.CreationTimestamp.Time) {
					latest = &rep
				}
			}
		}
		if latest != nil && time.Since(latest.CreationTimestamp.Time) < 2*time.Minute {
			log.Info("🛑 Skipping report creation — last report is recent", "age", time.Since(latest.CreationTimestamp.Time))
			ci.Status.LastRunTime = metav1.Now()
			ci.Status.Message = "Recent inventory already reported"
			_ = r.Status().Patch(ctx, ci, client.MergeFrom(original))
			return ctrl.Result{}, nil
		}
	}

	// Create the report resource (spec only)
	report := &v1alpha1.CloudInventoryReport{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: ci.Name + "-" + "k8s" + "-",
			Namespace:    ci.Namespace,
		},
		Spec: v1alpha1.CloudInventoryReportSpec{
			SourceRef: corev1.ObjectReference{
				Kind:       "CloudInventory",
				APIVersion: "openproject.org/v1alpha1",
				Name:       ci.Name,
				Namespace:  ci.Namespace,
			},
			Timestamp: metav1.Now(),
		},
	}
	if err := r.Create(ctx, report); err != nil {
		log.Error(err, "failed to create CloudInventoryReport")
		ci.Status.LastFailedTime = &metav1.Time{Time: time.Now()}
		_ = r.Status().Patch(ctx, ci, client.MergeFrom(original))
		return ctrl.Result{RequeueAfter: 10 * time.Minute}, nil
	}
	log.Info("Created empty CloudInventoryReport", "name", report.Name)

	// Patch the report’s status with images + summary
	reportCopy := report.DeepCopy()
	report.Status = v1alpha1.CloudInventoryReportStatus{
		ContainerImages: images,
		Summary:         summary,
	}
	if err := r.Status().Patch(ctx, report, client.MergeFrom(reportCopy)); err != nil {
		log.Error(err, "failed to patch CloudInventoryReport status", "name", report.Name)
		ci.Status.LastFailedTime = &metav1.Time{Time: time.Now()}
		_ = r.Status().Patch(ctx, ci, client.MergeFrom(original))
		return ctrl.Result{RequeueAfter: 10 * time.Minute}, nil
	}
	log.Info("✅ CloudInventoryReport created and status patched", "name", report.Name)

	// Finally, patch our CloudInventory CR’s status
	ci.Status.LastFailedTime = nil
	ci.Status.LastRunTime = metav1.Now()
	ci.Status.LastRunSuccess = true
	ci.Status.ItemCount = summary["pods"]
	ci.Status.Summary = summary
	ci.Status.Message = fmt.Sprintf("Kubernetes inventory complete for cluster %s", clusterName)

	if err := r.Status().Patch(ctx, ci, client.MergeFrom(original)); err != nil {
		log.Error(err, "Failed to patch Kubernetes inventory status")
		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: 10 * time.Minute}, nil
}

// buildRemoteKubeConfig reads kubeconfig from Secret and returns a *rest.Config
func (r *CloudInventoryReconciler) buildRemoteKubeConfig(ctx context.Context, ci *v1alpha1.CloudInventory, log logr.Logger) (*rest.Config, string, error) {
	ref := ci.Spec.Kubernetes.KubeconfigSecretRef
	secret := &corev1.Secret{}

	if err := r.Get(ctx, client.ObjectKey{Namespace: ci.Namespace, Name: ref.Name}, secret); err != nil {
		return nil, "", fmt.Errorf("failed to get kubeconfig secret: %w", err)
	}

	raw, ok := secret.Data[ref.Key]
	if !ok {
		return nil, "", fmt.Errorf("kubeconfig secret missing key %q", ref.Key)
	}

	config, err := clientcmd.RESTConfigFromKubeConfig(raw)
	if err != nil {
		return nil, "", fmt.Errorf("invalid kubeconfig: %w", err)
	}

	return config, parseClusterNameFromKubeconfig(raw), nil
}

// parseContainerImageWithDigest parses image into its repo, tag & sha, used above
func parseContainerImageWithDigest(image, imageID string) v1alpha1.ContainerImageInfo {
	var repo, version, sha string

	// Parse the image repo and tag
	tagParts := strings.Split(image, ":")
	repo = tagParts[0]
	if len(tagParts) > 1 {
		version = tagParts[1]
	}

	// Extract SHA from ImageID if available
	if strings.Contains(imageID, "@sha256:") {
		parts := strings.Split(imageID, "@")
		if len(parts) > 1 {
			sha = parts[1]
		}
	}

	return v1alpha1.ContainerImageInfo{
		Image:      image,
		Repository: repo,
		Version:    version,
		SHA:        sha,
	}
}

// parseClusterNameFromKubeconfig returns the name of the first cluster found
func parseClusterNameFromKubeconfig(kubeconfig []byte) string {
	config, err := clientcmd.Load(kubeconfig)
	if err != nil || len(config.Clusters) == 0 {
		return "remote"
	}

	for name := range config.Clusters {
		return name
	}
	return "remote"
}

// parseContainerImage parses image into repository, version, and sha
func parseContainerImage(image string) v1alpha1.ContainerImageInfo {
	var repo, version, sha string

	// Format: repo[:tag][@sha256:...]
	mainParts := strings.Split(image, "@")
	namePart := mainParts[0]

	if len(mainParts) > 1 {
		sha = mainParts[1]
	}

	tagParts := strings.Split(namePart, ":")
	repo = tagParts[0]
	if len(tagParts) > 1 {
		version = tagParts[1]
	}

	return v1alpha1.ContainerImageInfo{
		Image:      image,
		Repository: repo,
		Version:    version,
		SHA:        sha,
	}
}
