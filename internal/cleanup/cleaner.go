package cleanup

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"sync"

	"github.com/vbauerster/mpb/v8"
	"github.com/vbauerster/mpb/v8/decor"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"

	"github.com/fl64/cleanup-namespace/internal/client"
)

type Stats struct {
	TotalResourceTypes int
	ProcessedTypes     int
	TotalResources     int
	DeletedResources   int
	Errors             int
	ResourceTypeStats  map[string]int
	mu                 sync.Mutex
}

type Cleaner struct {
	clients *client.Clients
	dryRun  bool
}

func NewCleaner(clients *client.Clients, dryRun bool) *Cleaner {
	return &Cleaner{
		clients: clients,
		dryRun:  dryRun,
	}
}

func NamespaceExists(ctx context.Context, clients *client.Clients, namespace string) (bool, error) {
	_, err := clients.Kubernetes.CoreV1().Namespaces().Get(ctx, namespace, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (c *Cleaner) Cleanup(ctx context.Context, namespace string, workers int, includePatterns, excludePatterns []string) error {
	apiResources, err := c.getNamespacedResources(ctx)
	if err != nil {
		return fmt.Errorf("failed to get API resources: %w", err)
	}

	stats := &Stats{
		TotalResourceTypes: len(apiResources),
		ResourceTypeStats:  make(map[string]int),
	}

	includeRegexes, err := compilePatterns(includePatterns)
	if err != nil {
		return fmt.Errorf("failed to compile include patterns: %w", err)
	}
	excludeRegexes, err := compilePatterns(excludePatterns)
	if err != nil {
		return fmt.Errorf("failed to compile exclude patterns: %w", err)
	}

	var filteredResources []schema.GroupVersionResource
	for _, apiRes := range apiResources {
		if apiRes.Resource == "events" || apiRes.Resource == "events.events.k8s.io" {
			continue
		}

		if len(includeRegexes) > 0 {
			matched := false
			resourceName := apiRes.Resource
			resourceTypeName := c.formatResourceTypeName(apiRes)
			for _, regex := range includeRegexes {
				if regex.MatchString(resourceName) || regex.MatchString(resourceTypeName) {
					matched = true
					break
				}
			}
			if !matched {
				continue
			}
		}

		if len(excludeRegexes) > 0 {
			excluded := false
			resourceName := apiRes.Resource
			resourceTypeName := c.formatResourceTypeName(apiRes)
			for _, regex := range excludeRegexes {
				if regex.MatchString(resourceName) || regex.MatchString(resourceTypeName) {
					excluded = true
					break
				}
			}
			if excluded {
				continue
			}
		}

		filteredResources = append(filteredResources, apiRes)
	}

	if len(filteredResources) == 0 {
		return nil
	}

	p := mpb.New(mpb.WithWidth(80))
	stats.TotalResourceTypes = len(filteredResources)

	overallBar, _ := p.Add(int64(len(filteredResources)),
		mpb.BarStyle().Lbound("[").Filler("=").Tip(">").Padding("-").Rbound("]").Build(),
		mpb.PrependDecorators(
			decor.Name("Scanned types", decor.WC{W: 20, C: decor.DSyncWidth}),
			decor.CountersNoUnit("%d / %d", decor.WC{W: 10, C: decor.DSyncWidth}),
		),
		mpb.AppendDecorators(
			decor.Percentage(decor.WC{W: 5, C: decor.DSyncWidth}),
		),
	)

	type ResourceGroup struct {
		ResourceType string
		Infos        []ResourceInfo
	}
	var allResourceInfos []ResourceGroup

	var overallErrors []error
	for _, apiRes := range filteredResources {
		resourceTypeName := c.formatResourceTypeName(apiRes)

		typeStats, resourceInfos, err := c.cleanupResourceType(ctx, namespace, apiRes, workers, stats, p, resourceTypeName)
		if err != nil {
			errMsg := fmt.Errorf("%s: %w", resourceTypeName, err)
			overallErrors = append(overallErrors, errMsg)
			stats.mu.Lock()
			stats.Errors++
			stats.mu.Unlock()
		}

		if len(resourceInfos) > 0 {
			allResourceInfos = append(allResourceInfos, ResourceGroup{
				ResourceType: apiRes.Resource,
				Infos:        resourceInfos,
			})
		}

		stats.mu.Lock()
		stats.DeletedResources += typeStats
		stats.mu.Unlock()

		overallBar.Increment()
	}

	p.Wait()

	var hasErrors bool
	for _, resourceGroup := range allResourceInfos {
		for _, info := range resourceGroup.Infos {
			fmt.Printf("  %s/%s\n", resourceGroup.ResourceType, info.Name)
			if len(info.OwnerRefs) > 0 {
				fmt.Printf("    owner-> %s\n", strings.Join(info.OwnerRefs, ", "))
			}
			if len(info.Finalizers) > 0 {
				fmt.Printf("    finalizers -> %s\n", strings.Join(info.Finalizers, "; "))
			}
			if len(info.Errors) > 0 {
				hasErrors = true
			}
		}
	}

	var errorCount int
	if hasErrors {
		fmt.Println("\n⚠ Errors encountered during cleanup:")
		for _, resourceGroup := range allResourceInfos {
			for _, info := range resourceGroup.Infos {
				if len(info.Errors) == 0 {
					continue
				}
				errorCount++
				fmt.Printf("  ✗ %s/%s\n", resourceGroup.ResourceType, info.Name)
				for _, e := range info.Errors {
					fmt.Printf("      %s\n", e)
				}
			}
		}
	}

	if errorCount > 0 {
		return fmt.Errorf("cleanup completed with errors: %d resource(s) had problems", errorCount)
	}
	if len(overallErrors) > 0 {
		return fmt.Errorf("cleanup completed with errors: %d resource type(s) failed; first error: %w", len(overallErrors), overallErrors[0])
	}

	return nil
}

func (c *Cleaner) formatResourceTypeName(gvr schema.GroupVersionResource) string {
	if gvr.Group == "" {
		return gvr.Resource
	}
	return fmt.Sprintf("%s/%s", gvr.Group, gvr.Resource)
}

func compilePatterns(patterns []string) ([]*regexp.Regexp, error) {
	if len(patterns) == 0 {
		return nil, nil
	}

	regexes := make([]*regexp.Regexp, 0, len(patterns))
	for _, pattern := range patterns {
		if pattern == "" {
			continue
		}
		re, err := regexp.Compile(pattern)
		if err != nil {
			return nil, fmt.Errorf("invalid regex pattern %q: %w", pattern, err)
		}
		regexes = append(regexes, re)
	}
	return regexes, nil
}

func (c *Cleaner) getNamespacedResources(ctx context.Context) ([]schema.GroupVersionResource, error) {
	discoveryClient := c.clients.Kubernetes.Discovery()
	apiResourceLists, err := discoveryClient.ServerPreferredResources()
	if err != nil {
		return nil, err
	}

	var resources []schema.GroupVersionResource
	seen := make(map[string]bool)

	for _, apiResourceList := range apiResourceLists {
		if len(apiResourceList.APIResources) == 0 {
			continue
		}

		gv, err := schema.ParseGroupVersion(apiResourceList.GroupVersion)
		if err != nil {
			continue
		}

		for _, apiResource := range apiResourceList.APIResources {
			if !apiResource.Namespaced {
				continue
			}

			if strings.Contains(apiResource.Name, "/") {
				continue
			}

			key := fmt.Sprintf("%s/%s/%s", gv.Group, gv.Version, apiResource.Name)
			if seen[key] {
				continue
			}
			seen[key] = true

			resources = append(resources, schema.GroupVersionResource{
				Group:    gv.Group,
				Version:  gv.Version,
				Resource: apiResource.Name,
			})
		}
	}

	return resources, nil
}

type ResourceInfo struct {
	Name       string
	OwnerRefs  []string
	Finalizers []string
	Errors     []string
}

func (c *Cleaner) cleanupResourceType(ctx context.Context, namespace string, gvr schema.GroupVersionResource, workers int, stats *Stats, p *mpb.Progress, resourceTypeName string) (int, []ResourceInfo, error) {
	resourceClient := c.clients.Dynamic.Resource(gvr).Namespace(namespace)

	resources, err := resourceClient.List(ctx, metav1.ListOptions{})
	if err != nil {
		return 0, nil, fmt.Errorf("failed to list resources: %w", err)
	}

	if len(resources.Items) == 0 {
		return 0, nil, nil
	}

	resourceCount := len(resources.Items)

	resourceBar, _ := p.Add(int64(resourceCount),
		mpb.BarStyle().Lbound("[").Filler("=").Tip(">").Padding("-").Rbound("]").Build(),
		mpb.PrependDecorators(
			decor.Name(resourceTypeName, decor.WC{W: 30, C: decor.DSyncWidth}),
			decor.CountersNoUnit("%d / %d", decor.WC{W: 10, C: decor.DSyncWidth}),
		),
		mpb.AppendDecorators(
			decor.Percentage(decor.WC{W: 5, C: decor.DSyncWidth}),
		),
	)

	infoCh := make(chan ResourceInfo, resourceCount)

	var wg sync.WaitGroup
	semaphore := make(chan struct{}, workers)
	var mu sync.Mutex
	var errors []error
	deletedCount := 0

	for i := range resources.Items {
		wg.Add(1)
		semaphore <- struct{}{}

		go func(resource unstructured.Unstructured) {
			defer wg.Done()
			defer func() { <-semaphore }()

			if err := c.deleteResource(ctx, namespace, gvr, &resource, infoCh, c.dryRun); err != nil {
				mu.Lock()
				errors = append(errors, fmt.Errorf("failed to delete %s: %w", resource.GetName(), err))
				mu.Unlock()
			} else {
				if !c.dryRun {
					mu.Lock()
					deletedCount++
					mu.Unlock()
				}
			}

			resourceBar.Increment()
		}(resources.Items[i])
	}

	wg.Wait()
	close(infoCh)

	var resourceInfos []ResourceInfo
	for info := range infoCh {
		resourceInfos = append(resourceInfos, info)
	}

	if len(errors) > 0 {
		return deletedCount, resourceInfos, fmt.Errorf("encountered %d error(s): %v", len(errors), errors[0])
	}

	return deletedCount, resourceInfos, nil
}

func (c *Cleaner) deleteResource(ctx context.Context, namespace string, gvr schema.GroupVersionResource, resource *unstructured.Unstructured, infoCh chan<- ResourceInfo, dryRun bool) error {
	name := resource.GetName()
	resourceClient := c.clients.Dynamic.Resource(gvr).Namespace(namespace)

	info := ResourceInfo{
		Name: name,
	}

	ownerRefs := resource.GetOwnerReferences()
	if len(ownerRefs) > 0 {
		for _, ref := range ownerRefs {
			info.OwnerRefs = append(info.OwnerRefs, fmt.Sprintf("%s/%s", strings.ToLower(ref.Kind), ref.Name))
		}
	}

	finalizers := resource.GetFinalizers()
	if len(finalizers) > 0 {
		info.Finalizers = finalizers
	}

	if dryRun {
		infoCh <- info
		return nil
	}

	// Попытка снять финалайзеры: сначала Update, если не получилось — Patch (fallback)
	patch := []map[string]interface{}{
		{
			"op":   "remove",
			"path": "/metadata/finalizers",
		},
	}
	patchBytes, _ := json.Marshal(patch)

	errSet := make(map[string]struct{})
	addError := func(msg string) {
		if _, exists := errSet[msg]; !exists {
			errSet[msg] = struct{}{}
			info.Errors = append(info.Errors, msg)
		}
	}

	// Попытка снять финалайзеры: Update, затем Patch как fallback
	finalizersRemoved := false
	current, getErr := resourceClient.Get(ctx, name, metav1.GetOptions{})
	if getErr == nil && len(current.GetFinalizers()) > 0 {
		current.SetFinalizers([]string{})
		if _, err := resourceClient.Update(ctx, current, metav1.UpdateOptions{}); err == nil {
			finalizersRemoved = true
		} else {
			if !isNotFoundError(err) {
				addError(err.Error())
			}
			if _, patchErr := resourceClient.Patch(ctx, name, types.JSONPatchType, patchBytes, metav1.PatchOptions{}); patchErr == nil {
				finalizersRemoved = true
			} else if !isNotFoundError(patchErr) {
				addError(patchErr.Error())
			}
		}
	}

	// Удаление ресурса
	gracePeriod := int64(0)
	deleteErr := resourceClient.Delete(ctx, name, metav1.DeleteOptions{
		GracePeriodSeconds: &gracePeriod,
	})
	if deleteErr != nil && !isNotFoundError(deleteErr) {
		addError(deleteErr.Error())
	}

	// Повторная попытка снять финалайзеры после удаления
	if !finalizersRemoved {
		if _, err := resourceClient.Patch(ctx, name, types.JSONPatchType, patchBytes, metav1.PatchOptions{}); err != nil {
			if !isNotFoundError(err) {
				addError(err.Error())
			}
		}
	}

	infoCh <- info

	if deleteErr != nil && !isNotFoundError(deleteErr) {
		return fmt.Errorf("failed to delete %s/%s: %w", gvr.Resource, name, deleteErr)
	}

	return nil
}

func isNotFoundError(err error) bool {
	if err == nil {
		return false
	}
	return apierrors.IsNotFound(err)
}
