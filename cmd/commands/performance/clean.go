package performance

import (
	"context"
	"github.com/davenury/ucac/cmd/commands/utils"
	"github.com/spf13/cobra"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

var cleanupNamespace string

func createCleanupCommand() *cobra.Command {
	var cmd = &cobra.Command{
		Use: "cleanup",
		Short: "Cleanups performance tests without deleting apps",
		Run: func(cmd *cobra.Command, args []string) {

			clientset, err := utils.GetClientset()
			if err != nil {
				panic(err)
			}

			deleteJob(clientset)
			deleteService(clientset)
			deleteConfigmap(clientset)
		},
	}

	cmd.Flags().StringVarP(&cleanupNamespace, "namespace", "n", "default", "Namespace to cleanup in")

	return cmd
}

func deleteJob(clientset *kubernetes.Clientset) {
	clientset.BatchV1().Jobs(cleanupNamespace).DeleteCollection(context.Background(), metav1.DeleteOptions{}, metav1.ListOptions{
		LabelSelector: "project=ucac",
	})
}

func deleteConfigmap(clientset *kubernetes.Clientset) {
	clientset.CoreV1().ConfigMaps(cleanupNamespace).DeleteCollection(context.Background(), metav1.DeleteOptions{}, metav1.ListOptions{
		LabelSelector: "project=ucac",
	})
}

func deleteService(clientset *kubernetes.Clientset) {

	serviceClient := clientset.CoreV1().Services(cleanupNamespace)
	services, _ := serviceClient.List(context.Background(), metav1.ListOptions{
		LabelSelector: "project=ucac",
	})

	for _, service := range services.Items {
		serviceClient.Delete(context.Background(), service.ObjectMeta.Name, metav1.DeleteOptions{})
	}

}