package cmd

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
	"k8s.io/client-go/util/homedir"

	"github.com/fl64/cleanup-namespace/internal/cleanup"
	"github.com/fl64/cleanup-namespace/internal/client"
)

var (
	namespace  string
	kubeconfig string
	workers    int
	include    []string
	exclude    []string
	dryRun     bool
)

var rootCmd = &cobra.Command{
	Use:   "cleanup-namespace",
	Short: "Cleanup all resources in a Kubernetes namespace",
	Long: `Cleanup-ns removes all resources from a specified Kubernetes namespace.
It handles finalizers and supports filtering resources by regex patterns.`,
	SilenceUsage:  true,
	SilenceErrors: true,
	RunE:          runCleanup,
}

func init() {
	var kubeconfigDefault string
	if home := homedir.HomeDir(); home != "" {
		kubeconfigDefault = filepath.Join(home, ".kube", "config")
	}

	rootCmd.Flags().StringVarP(&namespace, "namespace", "n", "", "namespace to cleanup (required)")
	rootCmd.Flags().StringVarP(&kubeconfig, "kubeconfig", "k", kubeconfigDefault, "absolute path to the kubeconfig file")
	rootCmd.Flags().IntVarP(&workers, "workers", "w", 50, "number of concurrent workers for resource deletion")
	rootCmd.Flags().StringSliceVarP(&include, "include", "i", []string{}, "comma-separated list of regex patterns to include resource types (e.g., 'pods,services')")
	rootCmd.Flags().StringSliceVarP(&exclude, "exclude", "e", []string{}, "comma-separated list of regex patterns to exclude resource types (e.g., 'secrets,configmaps')")
	rootCmd.Flags().BoolVarP(&dryRun, "dry-run", "d", false, "preview resources to be deleted without actually deleting them")

	_ = rootCmd.MarkFlagRequired("namespace")
}

func runCleanup(cmd *cobra.Command, args []string) error {
	if namespace == "" {
		return fmt.Errorf("namespace is required")
	}

	if workers < 1 {
		return fmt.Errorf("workers must be >= 1")
	}

	ctx := context.Background()

	clients, err := client.NewClients(kubeconfig, workers)
	if err != nil {
		return fmt.Errorf("failed to create Kubernetes clients: %w", err)
	}

	exists, err := cleanup.NamespaceExists(ctx, clients, namespace)
	if err != nil {
		return fmt.Errorf("failed to check namespace: %w", err)
	}
	if !exists {
		return fmt.Errorf("namespace %s doesn't exist", namespace)
	}

	includePatterns := make([]string, 0, len(include))
	for _, pattern := range include {
		if trimmed := strings.TrimSpace(pattern); trimmed != "" {
			includePatterns = append(includePatterns, trimmed)
		}
	}

	excludePatterns := make([]string, 0, len(exclude))
	for _, pattern := range exclude {
		if trimmed := strings.TrimSpace(pattern); trimmed != "" {
			excludePatterns = append(excludePatterns, trimmed)
		}
	}

	if dryRun {
		fmt.Println("DRY-RUN MODE: No resources will be deleted")
	}

	cleaner := cleanup.NewCleaner(clients, dryRun)
	if err := cleaner.Cleanup(ctx, namespace, workers, includePatterns, excludePatterns); err != nil {
		return err
	}

	if dryRun {
		fmt.Println("\nDry-run completed successfully (no resources were deleted)")
	} else {
		fmt.Println("\n✔ Cleanup completed successfully")
	}
	return nil
}

func Execute() error {
	return rootCmd.Execute()
}

func Run() error {
	return Execute()
}
