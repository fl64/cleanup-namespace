package client

import (
	"path/filepath"

	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
)

type Clients struct {
	Kubernetes *kubernetes.Clientset
	Dynamic    dynamic.Interface
	Config     *rest.Config
}

func NewClients(kubeconfigPath string, maxWorkers int) (*Clients, error) {
	if kubeconfigPath == "" {
		if home := homedir.HomeDir(); home != "" {
			kubeconfigPath = filepath.Join(home, ".kube", "config")
		}
	}

	config, err := clientcmd.BuildConfigFromFlags("", kubeconfigPath)
	if err != nil {
		return nil, err
	}

	if maxWorkers > 0 {
		config.QPS = float32(maxWorkers * 2)
		config.Burst = maxWorkers * 5
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}

	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		return nil, err
	}

	return &Clients{
		Kubernetes: clientset,
		Dynamic:    dynamicClient,
		Config:     config,
	}, nil
}
