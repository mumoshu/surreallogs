package metadata

import (
	"context"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

// KubeProvider is a metadata provider for Kubernetes pods.
// It enriches each log entry with pod metadata obtained from Kubernetes API using client-go.
// The provider assumes "path" in the log entry is a Kubernetes pod log file path.
//
//	/var/log/pods/<namespace>_<pod_name>_<pod uid>/<container_name>/<index>.log
//
// Note:
// - <index> is 0 for the current container, and
// - The log file is a symbolic link to /var/lib/docker/containers/... in case it's a Docker container.
//
// It parses the path to obtain the pod name and namespace, and fetches the pod metadata from Kubernetes API.
type KubeProvider struct {
	clientset *kubernetes.Clientset
}

// NewKube initializes a new KubeProvider with a Kubernetes client.
func NewKube(kubeconfig string) (*KubeProvider, error) {
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		return nil, err
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}
	return &KubeProvider{clientset: clientset}, nil
}

func (p *KubeProvider) Fetch(ctx context.Context, path string, m map[string]interface{}) error {
	// Parse the path to extract namespace, pod name, and container name
	parts := strings.Split(path, "/")
	if len(parts) < 6 {
		return nil // Invalid path format
	}
	namespace := parts[3]
	podName := parts[4]
	containerName := parts[5]

	// Fetch pod metadata
	pod, err := p.clientset.CoreV1().Pods(namespace).Get(ctx, podName, metav1.GetOptions{})
	if err != nil {
		return err
	}

	// Enrich the metadata map with pod information
	m["namespace"] = pod.Namespace
	m["pod_name"] = pod.Name
	m["container_name"] = containerName
	return nil
}
