package commands

import (
	"context"
	"fmt"
	"strconv"

	"github.com/spf13/cobra"
	"github.com/google/uuid"
	appsv1 "k8s.io/api/apps/v1"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	// "k8s.io/client-go/util/retry"

	// maybe won't use
	// "k8s.io/client-go/plugin/pkg/client/auth"
)

const ratisPort = 10024
const servicePort = 8080

var numberOfPeersInPeerset int
var deployNamespace string

func CreateDeployCommand() *cobra.Command {

	var deployCommand = &cobra.Command{
		Use: "deploy",
		Short: "deploy application to cluster",
		Run: func(cmd *cobra.Command, args []string) {
			for i := 1; i <= numberOfPeersInPeerset; i++ {
				peerId := strconv.Itoa(i)

				peerConfig := PeerConfig{
					PeerId: peerId,
					PeersetId: "1",
					PeersInPeerset: numberOfPeersInPeerset,
				}

				deploySinglePeerService(deployNamespace, peerConfig, ratisPort + i)
				
				deploySinglePeerConfigMap(deployNamespace, peerConfig)

				deploySinglePeerDeployment(deployNamespace, peerConfig)

				fmt.Printf("Deployed app of peer %s from peerset %s\n", peerConfig.PeerId, peerConfig.PeersetId)
			}
		},
	}

	deployCommand.Flags().IntVar(&numberOfPeersInPeerset, "peersInPeerset", 0, "Number of peers in single peerset")
	deployCommand.MarkFlagRequired("peersInPeerset")

	deployCommand.Flags().StringVarP(&deployNamespace, "namespace", "n", "default", "Namespace to deploy cluster to")

	return deployCommand

}

func deploySinglePeerDeployment(namespace string, peerConfig PeerConfig) {

	clientset, err := GetClientset()
	if err != nil {
		panic(err)
	}

	deploymentClient := clientset.AppsV1().Deployments(namespace)

	oneReplicaPointer := int32(1)

	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name: DeploymentName(peerConfig),
			Namespace: namespace,
			Labels: map[string]string{
				"project": "ucac",
			},
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &oneReplicaPointer,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"peerId": peerConfig.PeerId,
					"peersetId": peerConfig.PeersetId,
				},
			},
			Template: createPodTemplate(peerConfig),
		},
	}

	fmt.Println("Creating deployment...")
	result, err := deploymentClient.Create(context.TODO(), deployment, metav1.CreateOptions{})
	if err != nil {
		panic(err)
	}
	fmt.Printf("Created deployment: %q.\n", result.GetObjectMeta().GetName())

}

func createPodTemplate(peerConfig PeerConfig) apiv1.PodTemplateSpec {
	containerName := ContainerName(peerConfig)

	return apiv1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Name: containerName,
			Labels: map[string]string{
				"peerId": peerConfig.PeerId,
				"peersetId": peerConfig.PeersetId,
			},
			Annotations: map[string]string{
				"prometheus.io/scrape": "true",
				"prometheus.io/port": "8080",
				"prometheus.io/path": "/_meta/metrics",
			},
		},
		Spec: apiv1.PodSpec{
			Containers: []apiv1.Container{
				createSingleContainer(containerName, peerConfig),
			},
		},
	}
}

func createSingleContainer(containerName string, peerConfig PeerConfig) apiv1.Container{
	return apiv1.Container{
		Name: containerName,
		Image: "davenury/ucac",
		Ports: []apiv1.ContainerPort{
			{
				ContainerPort: 8080,
			},
		},
		EnvFrom: []apiv1.EnvFromSource{
			{
				ConfigMapRef: &apiv1.ConfigMapEnvSource{
					LocalObjectReference: apiv1.LocalObjectReference{
						Name: ConfigMapName(peerConfig),
					},
				},
			},
		},
	}
}

func deploySinglePeerConfigMap(namespace string, peerConfig PeerConfig) {
	clientset, err := GetClientset()
	if err != nil {
		panic(err)
	}

	configMap := &apiv1.ConfigMap{
		TypeMeta: metav1.TypeMeta{
			Kind: "ConfigMap",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: ConfigMapName(peerConfig),
			Namespace: namespace,
			Labels: map[string]string{
				"project": "ucac",
			},
		},
		Data: map[string]string{
			"application_environment": "k8s",
			"PEERSET_ID": peerConfig.PeersetId,
			"RAFT_NODE_ID": peerConfig.PeerId,
			// TODO - this isn't the right way of passing list of lists to env 
			"RATIS_PEERS_ADDRESSES": fmt.Sprintf("[[%s]]", GenerateServicesForPeers(peerConfig, ratisPort)),
			"RATIS_CLUSTER_GROUPS_IDS": fmt.Sprintf("[%s]", uuid.New()),
			"GPAC_PEERS_ADDRESSES": fmt.Sprintf("[[%s]]", GenerateServicesForPeersStaticPort(peerConfig, servicePort)),
		},
	}

	clientset.CoreV1().ConfigMaps(namespace).Create(context.TODO(), configMap, metav1.CreateOptions{})
}

func deploySinglePeerService(namespace string, peerConfig PeerConfig, currentRatisPort int) {
	clientset, err := GetClientset()
	if err != nil {
		panic(err)
	}

	service := &apiv1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name: ServiceName(peerConfig),
			Namespace: namespace,
			Labels: map[string]string{
				"project": "ucac",
			},
		},
		Spec: apiv1.ServiceSpec{
			Selector: map[string]string{
				"peerId": peerConfig.PeerId,
				"peersetId": peerConfig.PeersetId,
			},
			Ports: []apiv1.ServicePort{
				{
					Name: "service",
					Port: servicePort,
					TargetPort: intstr.FromInt(servicePort),
				},
				{
					Name: "ratis",
					Port: int32(currentRatisPort),
					TargetPort: intstr.FromInt(currentRatisPort),
				},
			},
		},
	}

	clientset.CoreV1().Services(namespace).Create(context.TODO(), service, metav1.CreateOptions{})
}