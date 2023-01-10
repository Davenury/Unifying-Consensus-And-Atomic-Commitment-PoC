package commands

import (
	"context"
	"github.com/spf13/cobra"
	"github.com/davenury/ucac/cmd/commands/utils"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

func CreateCleanupCommand() *cobra.Command {

	var cleanupNamespace string

	var cmd = &cobra.Command{
		Use:   "cleanup",
		Short: "Cleanups deployment",
		Run: func(cmd *cobra.Command, args []string) {
			DoCleanup(cleanupNamespace)
		},
	}

	cmd.Flags().StringVarP(&cleanupNamespace, "namespace", "n", "default", "Namespace to clear deployemtns for")

	return cmd
}

func DoCleanup(namespace string) {
	clientset, err := utils.GetClientset()
	if err != nil {
		panic(err)
	}

	deleteDeployments(clientset, namespace)
	deleteConfigMaps(clientset, namespace)
	deleteService(clientset, namespace)
	deleteJob(clientset, namespace)
	deleteStatefulSets(clientset, namespace)
}

func deleteDeployments(clientset *kubernetes.Clientset, namespace string) {
	clientset.AppsV1().Deployments(namespace).DeleteCollection(context.Background(), metav1.DeleteOptions{}, metav1.ListOptions{
		LabelSelector: "project=ucac",
	})
}

func deleteConfigMaps(clientset *kubernetes.Clientset, namespace string) {
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

func deleteStatefulSets(clientset *kubernetes.Clientset, namespace string) {
	clientset.AppsV1().StatefulSets(namespace).DeleteCollection(context.Background(), metav1.DeleteOptions{}, metav1.ListOptions{
		LabelSelector: "project=ucac",
	})
}
func deleteJob(clientset *kubernetes.Clientset, cleanupNamespace string) {
	policy := metav1.DeletePropagationForeground
	clientset.BatchV1().Jobs(cleanupNamespace).DeleteCollection(context.Background(), metav1.DeleteOptions{
		PropagationPolicy: &policy,
	}, metav1.ListOptions{
		LabelSelector: "project=ucac",
	})
}
