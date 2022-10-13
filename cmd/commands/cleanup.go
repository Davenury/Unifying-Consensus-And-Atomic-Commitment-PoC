package commands

import (

	"context"
	"github.com/spf13/cobra"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

)

var cleanupNamespace string

func CreateCleanupCommand() *cobra.Command {

	var cmd = &cobra.Command{
		Use: "cleanup",
		Short: "Cleanups deployment",
		Run: func(cmd *cobra.Command, args []string) {

			clientset, err := GetClientset()
			if err != nil {
				panic(err)
			}

			deleteDeployments(clientset, cleanupNamespace)
			deleteConfigMaps(clientset, cleanupNamespace)
			deleteService(clientset, cleanupNamespace)
		},
	}

	cmd.Flags().StringVarP(&cleanupNamespace, "namespace", "n", "default", "Namespace to clear deployemtns for")

	return cmd
}

func deleteDeployments(clientset *kubernetes.Clientset, namespace string) {
	clientset.AppsV1().Deployments(namespace).DeleteCollection(context.TODO(), metav1.DeleteOptions{}, metav1.ListOptions{
		LabelSelector: "project=ucac",
	})
}

func deleteConfigMaps(clientset *kubernetes.Clientset, namespace string) {
	clientset.CoreV1().ConfigMaps(namespace).DeleteCollection(context.TODO(), metav1.DeleteOptions{}, metav1.ListOptions{
		LabelSelector: "project=ucac",
	})
}

func deleteService(clientset *kubernetes.Clientset, namespace string) {

	serviceClient := clientset.CoreV1().Services(namespace)
	services, _ := serviceClient.List(context.TODO(), metav1.ListOptions{
		LabelSelector: "project=ucac",
	})

	for _, service := range services.Items {
		serviceClient.Delete(context.TODO(), service.ObjectMeta.Name, metav1.DeleteOptions{})
	}

}