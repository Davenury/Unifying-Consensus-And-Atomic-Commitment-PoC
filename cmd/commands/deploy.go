package commands

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/spf13/cobra"
	appsv1 "k8s.io/api/apps/v1"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"strconv"
	"time"
)

const ratisPort = 10024
const servicePort = 8080

var numberOfPeersInPeersets []int
var deployNamespace string
var deployCreateNamespace bool
var waitForReady bool

func CreateDeployCommand() *cobra.Command {

	var deployCommand = &cobra.Command{
		Use:   "deploy",
		Short: "deploy application to cluster",
		Run: func(cmd *cobra.Command, args []string) {

			ratisPeers := GenerateServicesForPeers(numberOfPeersInPeersets, ratisPort)
			gpacPeers := GenerateServicesForPeersStaticPort(numberOfPeersInPeersets, servicePort)
			ratisGroups := make([]string, len(numberOfPeersInPeersets))
			for i := 0; i < len(numberOfPeersInPeersets); i++ {
				ratisGroups[i] = uuid.New().String()
			}

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

					deploySinglePeerConfigMap(deployNamespace, peerConfig, ratisPeers, gpacPeers)

					deploySinglePeerDeployment(deployNamespace, peerConfig)

					fmt.Printf("Deployed app of peer %s from peerset %s\n", peerConfig.PeerId, peerConfig.PeersetId)
				}
			}

			waitForPodsReady()
		},
	}

	deployCommand.Flags().IntSliceVar(&numberOfPeersInPeersets, "peers", make([]int, 0), "Number of peers in peersets; example usage '--peers=1,2,3'")
	deployCommand.Flags().BoolVarP(&deployCreateNamespace, "create-namespace", "", false, "Include if should create namespace")
	deployCommand.Flags().StringVarP(&deployNamespace, "namespace", "n", "default", "Namespace to deploy cluster to")
	deployCommand.Flags().BoolVarP(&waitForReady, "wait-for-ready", "", false, "Wait for deployment to be ready")

	return deployCommand

}

func waitForPodsReady() {
	for anyPodNotReady() {
		time.Sleep(1 * time.Second)
		fmt.Println("Waiting for pods to be ready")
	}
}

func anyPodNotReady() bool {

	clientset, err := GetClientset()
	if err != nil {
		panic(err)
	}

	pods, err := clientset.CoreV1().Pods(deployNamespace).List(context.Background(), metav1.ListOptions{})

	if len(pods.Items) == 0 {
		// we assume that we always have at least one pod
		return true
	}

	for _, pod := range pods.Items {
		if !pod.Status.ContainerStatuses[0].Ready {
			return true
		}
	}

	return false
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
				"project":   "ucac",
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
		ReadinessProbe: &apiv1.Probe{
			ProbeHandler: apiv1.ProbeHandler {
				HTTPGet: &apiv1.HTTPGetAction{
					Path: "/_meta/health",
					Port: intstr.FromInt(8080),
				},
			},
		},
		LivenessProbe: &apiv1.Probe{
			ProbeHandler: apiv1.ProbeHandler {
				HTTPGet: &apiv1.HTTPGetAction{
					Path: "/_meta/health",
					Port: intstr.FromInt(8080),
				},
			},
		},
	}
}

func deploySinglePeerConfigMap(namespace string, peerConfig PeerConfig, ratisPeers string, gpacPeers string) {
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
			"CONFIG_FILE":            "application-kubernetes.conf",
			"config_host":            ServiceAddress(peerConfig),
			"config_port":            "8080",
			"config_peersetId":       peerConfig.PeersetId,
			"config_peerId":          peerConfig.PeerId,
			"config_ratis_addresses": ratisPeers,
			"config_peers":           gpacPeers,
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
