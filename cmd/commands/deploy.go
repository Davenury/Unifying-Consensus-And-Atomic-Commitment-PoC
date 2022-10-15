package commands

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/spf13/cobra"
	appsv1 "k8s.io/api/apps/v1"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

const ratisPort = 10024
const servicePort = 8080

var numberOfPeersInPeersets []int
var deployNamespace string
var deployCreateNamespace bool

func CreateDeployCommand() *cobra.Command {

	var deployCommand = &cobra.Command{
		Use:   "deploy",
		Short: "deploy application to cluster",
		Run: func(cmd *cobra.Command, args []string) {

			ratisPeers := fmt.Sprintf("[%s]", GenerateServicesForPeers(numberOfPeersInPeersets, ratisPort, deployNamespace))
			gpacPeers := fmt.Sprintf("[%s]", GenerateServicesForPeersStaticPort(numberOfPeersInPeersets, servicePort, deployNamespace))
			ratisGroups := make([]string, len(numberOfPeersInPeersets))
			for i := 0; i < len(numberOfPeersInPeersets); i++ {
				ratisGroups[i] = uuid.New().String()
			}
			ratisGroupsString := strings.Join(ratisGroups[:], ",")

			if deployCreateNamespace {
				CreateNamespace(deployNamespace)
			}

			for idx, num := range numberOfPeersInPeersets {
				for i := 1; i <= num; i++ {
					peerId := strconv.Itoa(i)

					peerConfig := PeerConfig{
						PeerId:         peerId,
						PeersetId:      strconv.Itoa(idx + 1),
						PeersInPeerset: num,
						PeersetsConfig: numberOfPeersInPeersets,
					}

					deploySinglePeerService(deployNamespace, peerConfig, ratisPort+i)

					deploySinglePeerConfigMap(deployNamespace, peerConfig, ratisPeers, gpacPeers, ratisGroupsString)

					deploySinglePeerDeployment(deployNamespace, peerConfig)

					fmt.Printf("Deployed app of peer %s from peerset %s\n", peerConfig.PeerId, peerConfig.PeersetId)
				}
			}
		},
	}

	deployCommand.Flags().IntSliceVar(&numberOfPeersInPeersets, "peers", make([]int, 0), "Number of peers in peersets; example usage '--peers=1,2,3'")
	deployCommand.Flags().BoolVarP(&deployCreateNamespace, "create-namespace", "", false, "Include if should create namespace")
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
			Name:      DeploymentName(peerConfig),
			Namespace: namespace,
			Labels: map[string]string{
				"project": "ucac",
			},
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &oneReplicaPointer,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"peerId":    peerConfig.PeerId,
					"peersetId": peerConfig.PeersetId,
				},
			},
			Template: createPodTemplate(peerConfig),
		},
	}

	fmt.Println("Creating deployment...")
	result, err := deploymentClient.Create(context.Background(), deployment, metav1.CreateOptions{})
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
				"peerId":    peerConfig.PeerId,
				"peersetId": peerConfig.PeersetId,
				"app.name":  fmt.Sprintf("peer%s-peerset%s-app", peerConfig.PeerId, peerConfig.PeersetId),
			},
			Annotations: map[string]string{
				"prometheus.io/scrape": "true",
				"prometheus.io/port":   "8080",
				"prometheus.io/path":   "/_meta/metrics",
			},
		},
		Spec: apiv1.PodSpec{
			Containers: []apiv1.Container{
				createSingleContainer(containerName, peerConfig),
			},
		},
	}
}

func createSingleContainer(containerName string, peerConfig PeerConfig) apiv1.Container {
	return apiv1.Container{
		Name:  containerName,
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

func deploySinglePeerConfigMap(namespace string, peerConfig PeerConfig, ratisPeers string, gpacPeers string, ratisGroups string) {
	clientset, err := GetClientset()
	if err != nil {
		panic(err)
	}

	configMap := &apiv1.ConfigMap{
		TypeMeta: metav1.TypeMeta{
			Kind:       "ConfigMap",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      ConfigMapName(peerConfig),
			Namespace: namespace,
			Labels: map[string]string{
				"project": "ucac",
			},
		},
		Data: map[string]string{
			"application_environment":  "k8s",
			"CONFIG_FILE":              "application-kubernetes.conf",
			"PEERSET_ID":               peerConfig.PeersetId,
			"RAFT_NODE_ID":             peerConfig.PeerId,
			"RATIS_PEERS_ADDRESSES":    ratisPeers,
			"RATIS_CLUSTER_GROUPS_IDS": ratisGroups,
			"GPAC_PEERS_ADDRESSES":     gpacPeers,
		},
	}

	clientset.CoreV1().ConfigMaps(namespace).Create(context.Background(), configMap, metav1.CreateOptions{})
}

func deploySinglePeerService(namespace string, peerConfig PeerConfig, currentRatisPort int) {
	clientset, err := GetClientset()
	if err != nil {
		panic(err)
	}

	service := &apiv1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ServiceName(peerConfig),
			Namespace: namespace,
			Labels: map[string]string{
				"project": "ucac",
			},
		},
		Spec: apiv1.ServiceSpec{
			Selector: map[string]string{
				"app.name": fmt.Sprintf("peer%s-peerset%s-app", peerConfig.PeerId, peerConfig.PeersetId),
			},
			Ports: []apiv1.ServicePort{
				{
					Name:       "service",
					Port:       8080,
					TargetPort: intstr.FromInt(servicePort),
				},
				{
					Name:       "ratis",
					Port:       int32(currentRatisPort),
					TargetPort: intstr.FromInt(currentRatisPort),
				},
			},
		},
	}

	clientset.CoreV1().Services(namespace).Create(context.Background(), service, metav1.CreateOptions{})
}
