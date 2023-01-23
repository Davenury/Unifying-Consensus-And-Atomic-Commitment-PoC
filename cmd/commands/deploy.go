package commands

import (
	"context"
	"fmt"
	"github.com/davenury/ucac/cmd/commands/utils"
	"github.com/google/uuid"
	"github.com/spf13/cobra"
	appsv1 "k8s.io/api/apps/v1"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/api/resource"
	"strconv"
	"time"
)

const ratisPort = 10024
const servicePort = 8080

func CreateDeployCommand() *cobra.Command {

	var numberOfPeersInPeersets []int
	var deployNamespace string
	var deployCreateNamespace bool
	var waitForReadiness bool
	var imageName string


	var deployCommand = &cobra.Command{
		Use:   "deploy",
		Short: "deploy application to cluster",
		Run: func(cmd *cobra.Command, args []string) {
			DoDeploy(numberOfPeersInPeersets, deployCreateNamespace, deployNamespace, waitForReadiness, imageName)
		},
	}

	deployCommand.Flags().IntSliceVar(&numberOfPeersInPeersets, "peers", make([]int, 0), "Number of peers in peersets; example usage '--peers=1,2,3'")
	deployCommand.Flags().BoolVarP(&deployCreateNamespace, "create-namespace", "", false, "Include if should create namespace")
	deployCommand.Flags().StringVarP(&deployNamespace, "namespace", "n", "default", "Namespace to deploy cluster to")
	deployCommand.Flags().BoolVarP(&waitForReadiness, "wait-for-readiness", "", false, "Wait for deployment to be ready")
	deployCommand.Flags().StringVarP(&imageName, "image", "", "ghcr.io/davenury/ucac:latest", "A Docker image to be used in the deployment")

	return deployCommand

}

func DoDeploy(numberOfPeersInPeersets []int, createNamespace bool, namespace string, waitForReadiness bool, imageName string) {
	ratisPeers := utils.GenerateServicesForPeers(numberOfPeersInPeersets, ratisPort)
	gpacPeers := utils.GenerateServicesForPeersStaticPort(numberOfPeersInPeersets, servicePort)
	ratisGroups := make([]string, len(numberOfPeersInPeersets))
	for i := 0; i < len(numberOfPeersInPeersets); i++ {
		ratisGroups[i] = uuid.New().String()
	}

	if createNamespace {
		utils.CreateNamespace(namespace)
	}

	totalPeers := 0
	for idx, num := range numberOfPeersInPeersets {
		totalPeers += num
		for i := 0; i < num; i++ {
			peerId := strconv.Itoa(i)

			peerConfig := utils.PeerConfig{
				PeerId:         peerId,
				PeersetId:      strconv.Itoa(idx),
				PeersInPeerset: num,
				PeersetsConfig: numberOfPeersInPeersets,
			}

			deploySinglePeerService(namespace, peerConfig, ratisPort+i)

			deploySinglePeerConfigMap(namespace, peerConfig, ratisPeers, gpacPeers)

			deploySinglePeerDeployment(namespace, peerConfig, imageName)

			fmt.Printf("Deployed app of peer %s from peerset %s\n", peerConfig.PeerId, peerConfig.PeersetId)
		}
	}

	if waitForReadiness {
		waitForPodsReadiness(totalPeers, namespace)
	}
}

func waitForPodsReadiness(expectedPeers int, namespace string) {
	fmt.Println("Waiting for pods to be ready")
	deadline := time.Now().Add(3 * time.Minute)
	for anyPodNotReady(expectedPeers, namespace) {
		if time.Now().After(deadline) {
			panic("Timed out while waiting for pod readiness")
		}
		time.Sleep(1 * time.Second)
	}
}

func anyPodNotReady(expectedPeers int, namespace string) bool {

	clientset, err := utils.GetClientset()
	if err != nil {
		panic(err)
	}

	pods, err := clientset.CoreV1().Pods(namespace).List(context.Background(), metav1.ListOptions{
		LabelSelector: "project=ucac",
	})

	totalCount := len(pods.Items)
	notReadyCount := 0
	for _, pod := range pods.Items {
		containerStatuses := pod.Status.ContainerStatuses
		if len(containerStatuses) != 1 || !containerStatuses[0].Ready {
			notReadyCount += 1
		}
	}

	fmt.Printf("Ready pods: %d/%d (expected %d)\n", totalCount-notReadyCount, totalCount, expectedPeers)

	return totalCount != expectedPeers || notReadyCount > 0
}

func deploySinglePeerDeployment(namespace string, peerConfig utils.PeerConfig, imageName string) {

	clientset, err := utils.GetClientset()
	if err != nil {
		panic(err)
	}

	deploymentClient := clientset.AppsV1().Deployments(namespace)

	oneReplicaPointer := int32(1)

	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      utils.DeploymentName(peerConfig),
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
			Template: createPodTemplate(peerConfig, imageName),
		},
	}

	fmt.Println("Creating deployment...")
	result, err := deploymentClient.Create(context.Background(), deployment, metav1.CreateOptions{})
	if err != nil {
		panic(err)
	}
	fmt.Printf("Created deployment: %q.\n", result.GetObjectMeta().GetName())

}

func createPodTemplate(peerConfig utils.PeerConfig, imageName string) apiv1.PodTemplateSpec {
	containerName := utils.ContainerName(peerConfig)

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
				createSingleContainer(containerName, peerConfig, imageName),
			},
		},
	}
}

func createSingleContainer(containerName string, peerConfig utils.PeerConfig, imageName string) apiv1.Container {

	probe := &apiv1.Probe{
		ProbeHandler: apiv1.ProbeHandler{
			HTTPGet: &apiv1.HTTPGetAction{
				Path: "/_meta/health",
				Port: intstr.FromInt(8080),
			},
		},
	}

	return apiv1.Container{
		Name:  containerName,
		Image: imageName,
		Resources: apiv1.ResourceRequirements{
			Limits: apiv1.ResourceList{
				"cpu": resource.MustParse("700m"),
				"memory": resource.MustParse("750Mi"),
			},
			Requests: apiv1.ResourceList{
				"cpu": resource.MustParse("500m"),
				"memory": resource.MustParse("350Mi"),
			},
		},
		Args: []string{
			"-Xmx500m",
			"-Dcom.sun.management.jmxremote.port=9999",
		},
		Ports: []apiv1.ContainerPort{
			{
				ContainerPort: 8080,
			},
		},
		EnvFrom: []apiv1.EnvFromSource{
			{
				ConfigMapRef: &apiv1.ConfigMapEnvSource{
					LocalObjectReference: apiv1.LocalObjectReference{
						Name: utils.ConfigMapName(peerConfig),
					},
				},
			},
		},
		ReadinessProbe: probe,
		LivenessProbe:  probe,
	}
}

func deploySinglePeerConfigMap(namespace string, peerConfig utils.PeerConfig, ratisPeers string, gpacPeers string) {
	clientset, err := utils.GetClientset()
	if err != nil {
		panic(err)
	}

	configMap := &apiv1.ConfigMap{
		TypeMeta: metav1.TypeMeta{
			Kind:       "ConfigMap",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      utils.ConfigMapName(peerConfig),
			Namespace: namespace,
			Labels: map[string]string{
				"project": "ucac",
			},
		},
		Data: map[string]string{
			"CONFIG_FILE":            "application-kubernetes.conf",
			"config_host":            utils.ServiceAddress(peerConfig),
			"config_port":            "8080",
			"config_peersetId":       peerConfig.PeersetId,
			"config_peerId":          peerConfig.PeerId,
			"config_ratis_addresses": ratisPeers,
			"config_peers":           gpacPeers,
		},
	}

	clientset.CoreV1().ConfigMaps(namespace).Create(context.Background(), configMap, metav1.CreateOptions{})
}

func deploySinglePeerService(namespace string, peerConfig utils.PeerConfig, currentRatisPort int) {
	clientset, err := utils.GetClientset()
	if err != nil {
		panic(err)
	}

	service := &apiv1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      utils.ServiceName(peerConfig),
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
