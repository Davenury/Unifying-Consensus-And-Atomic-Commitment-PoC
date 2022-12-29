package performance

import (
	"context"
	"github.com/davenury/ucac/cmd/commands/utils"
	"github.com/spf13/cobra"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)


func createCleanupCommand() *cobra.Command {
	var cleanupNamespace string
	var cmd = &cobra.Command{
		Use: "cleanup",
		Short: "Cleanups performance tests without deleting apps",
		Run: func(cmd *cobra.Command, args []string) {
			DoPerformanceCleanup(cleanupNamespace)
		},
	}

	cmd.Flags().StringVarP(&cleanupNamespace, "namespace", "n", "default", "Namespace to cleanup in")

	return cmd
}

func DoPerformanceCleanup(namespace string) {
	clientset, err := utils.GetClientset()
	if err != nil {
		panic(err)
	}

	deleteJob(clientset, namespace)
	deleteService(clientset, namespace)
	deleteConfigmap(clientset, namespace)
}

func deleteJob(clientset *kubernetes.Clientset, namespace string) {
	policy := metav1.DeletePropagationForeground
	clientset.BatchV1().Jobs(namespace).DeleteCollection(context.Background(), metav1.DeleteOptions{
		PropagationPolicy: &policy,
	}, metav1.ListOptions{
		LabelSelector: "project=ucac",
	})
}

func deleteConfigmap(clientset *kubernetes.Clientset, namespace string) {
	clientset.CoreV1().ConfigMaps(namespace).DeleteCollection(context.Background(), metav1.DeleteOptions{}, metav1.ListOptions{
		LabelSelector: "project=ucac",
	})
}

func deleteService(clientset *kubernetes.Clientset, namespace string) {

	serviceClient := clientset.CoreV1().Services(namespace)
	services, _ := serviceClient.List(context.Background(), metav1.ListOptions{
		LabelSelector: "project=ucac",
	})

	for _, service := range services.Items {
		serviceClient.Delete(context.Background(), service.ObjectMeta.Name, metav1.DeleteOptions{})
	}

}