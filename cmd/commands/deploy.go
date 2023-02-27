package commands

import (
	"context"
	"fmt"
	"github.com/davenury/ucac/cmd/commands/utils"
	"github.com/google/uuid"
	"github.com/spf13/cobra"
	appsv1 "k8s.io/api/apps/v1"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
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
	var isMetricTest bool
	var createResources bool
	var proxyDelay string
	var proxyLimit string

	var deployCommand = &cobra.Command{
		Use:   "deploy",
		Short: "deploy application to cluster",
		Run: func(cmd *cobra.Command, args []string) {
			DoDeploy(numberOfPeersInPeersets, deployCreateNamespace, deployNamespace, waitForReadiness, imageName, isMetricTest, createResources, proxyDelay, proxyLimit)
		},
	}

	deployCommand.Flags().IntSliceVar(&numberOfPeersInPeersets, "peers", make([]int, 0), "Number of peers in peersets; example usage '--peers=1,2,3'")
	deployCommand.Flags().BoolVarP(&deployCreateNamespace, "create-namespace", "", false, "Include if should create namespace")
	deployCommand.Flags().StringVarP(&deployNamespace, "namespace", "n", "default", "Namespace to deploy cluster to")
	deployCommand.Flags().BoolVarP(&waitForReadiness, "wait-for-readiness", "", false, "Wait for deployment to be ready")
	deployCommand.Flags().StringVarP(&imageName, "image", "", "ghcr.io/davenury/ucac:latest", "A Docker image to be used in the deployment")
	deployCommand.Flags().BoolVar(&isMetricTest, "is-metric-test", false, "Determines whether add additional change related metrics. DO NOT USE WITH NORMAL TESTS!")
	deployCommand.Flags().BoolVar(&createResources, "create-resources", true, "Determines if pods should have limits and requests")
	deployCommand.Flags().StringVar(&proxyDelay, "proxy-delay", "0", "Delay in seconds for proxy, e.g. 0.2")
	deployCommand.Flags().StringVar(&proxyLimit, "proxy-limit", "0", "Bandwidth limit in bytes per second, e.g. 100, 2M")

	return deployCommand

}

func DoDeploy(numberOfPeersInPeersets []int, createNamespace bool, namespace string, waitForReadiness bool, imageName string, isMetricTest bool, createResources bool, proxyDelay string, proxyLimit string) {
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

			createPV(namespace, peerConfig)
			createPVC(namespace, peerConfig)
			createRedisConfigmap(namespace, peerConfig)

			deploySinglePeerService(namespace, peerConfig, ratisPort+i)

			deploySinglePeerConfigMap(namespace, peerConfig, ratisPeers, gpacPeers, isMetricTest)

			deploySinglePeerDeployment(namespace, peerConfig, imageName, createResources, proxyDelay, proxyLimit)

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
		if len(containerStatuses) != 3 || !areContainersReady(containerStatuses) {
			notReadyCount += 1
		}
	}

	fmt.Printf("Ready pods: %d/%d (expected %d)\n", totalCount-notReadyCount, totalCount, expectedPeers)

	return totalCount != expectedPeers || notReadyCount > 0
}

func areContainersReady(containerStatuses []apiv1.ContainerStatus) bool {
	for _, status := range containerStatuses {
		if !status.Ready {
			return false
		}
	}
	return true
}

func deploySinglePeerDeployment(namespace string, peerConfig utils.PeerConfig, imageName string, createResources bool, proxyDelay string, proxyLimit string) {
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
			Template: createPodTemplate(peerConfig, imageName, createResources, proxyDelay, proxyLimit, namespace),
		},
	}

	fmt.Println("Creating deployment...")
	result, err := deploymentClient.Create(context.Background(), deployment, metav1.CreateOptions{})
	if err != nil {
		panic(err)
	}
	fmt.Printf("Created deployment: %q.\n", result.GetObjectMeta().GetName())

}

func createPodTemplate(peerConfig utils.PeerConfig, imageName string, createResources bool, proxyDelay string, proxyLimit string, namespace string) apiv1.PodTemplateSpec {
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
				"prometheus.io/port":   "8081",
				"prometheus.io/path":   "/_meta/metrics",
			},
		},
		Spec: apiv1.PodSpec{
			Containers: []apiv1.Container{
				createSingleContainer(peerConfig, imageName, createResources),
				createProxyContainer(proxyDelay, proxyLimit),
				createRedisContainer(),
			},
			Volumes: []apiv1.Volume{
				{
					Name: "redis-data",
					VolumeSource: apiv1.VolumeSource{
						PersistentVolumeClaim: &apiv1.PersistentVolumeClaimVolumeSource{
							ClaimName: utils.PVCName(peerConfig, namespace),
						},
					},
				},
				{
					Name: "config",
					VolumeSource: apiv1.VolumeSource{
						ConfigMap: &apiv1.ConfigMapVolumeSource{
							LocalObjectReference: apiv1.LocalObjectReference{
								Name: utils.RedisConfigmapName(peerConfig),
							},
							Items: []apiv1.KeyToPath{
								{
									Key:  "redis-config",
									Path: "redis.conf",
								},
							},
						},
					},
				},
			},
		},
	}
}

func createRedisContainer() apiv1.Container {
	return apiv1.Container{
		Name:  "redis",
		Image: "redis:7.0-alpine",
		Command: []string{
			"redis-server",
			"/redis-master/redis.conf",
		},
		Env: []apiv1.EnvVar{
			{
				Name:  "MASTER",
				Value: "true",
			},
		},
		Ports: []apiv1.ContainerPort{
			{
				ContainerPort: 6379,
			},
		},
		VolumeMounts: []apiv1.VolumeMount{
			{
				Name:      "redis-data",
				MountPath: "/data",
			},
			{
				Name:      "config",
				MountPath: "/redis-master",
			},
		},
	}
}

func createProxyContainer(delay string, limit string) apiv1.Container {
	return apiv1.Container{
		Name:  "proxy",
		Image: "lovelysystems/throttling-proxy-docker:latest",
		Ports: []apiv1.ContainerPort{
			{
				ContainerPort: 8080,
			},
		},
		Env: []apiv1.EnvVar{
			{
				Name:  "UPSTREAM",
				Value: "http://localhost:8081",
			},
			{
				Name:  "DELAY",
				Value: delay,
			},
			{
				Name:  "LIMIT",
				Value: limit,
			},
		},
	}
}
func createSingleContainer(peerConfig utils.PeerConfig, imageName string, createResources bool) apiv1.Container {

	probe := &apiv1.Probe{
		ProbeHandler: apiv1.ProbeHandler{
			HTTPGet: &apiv1.HTTPGetAction{
				Path: "/_meta/health",
				Port: intstr.FromInt(8081),
			},
		},
	}

	resources := apiv1.ResourceRequirements{}
	if createResources {
		resources = apiv1.ResourceRequirements{
			Limits: apiv1.ResourceList{
				"cpu":    resource.MustParse("700m"),
				"memory": resource.MustParse("750Mi"),
			},
			Requests: apiv1.ResourceList{
				"cpu":    resource.MustParse("500m"),
				"memory": resource.MustParse("350Mi"),
			},
		}
	}

	return apiv1.Container{
		Name:      "application",
		Image:     imageName,
		Resources: resources,
		Args: []string{
			"-Xmx500m",
			"-Dcom.sun.management.jmxremote.port=9999",
			"-Dlogback.configurationFile=./src/main/resources/loki.xml",
		},
		Ports: []apiv1.ContainerPort{
			{
				ContainerPort: 8081,
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

func deploySinglePeerConfigMap(namespace string, peerConfig utils.PeerConfig, ratisPeers string, gpacPeers string, isMetricTest bool) {
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
			"CONFIG_FILE":                  "application-kubernetes.conf",
			"config_host":                  utils.ServiceAddress(peerConfig),
			"config_port":                  "8081",
			"config_peersetId":             peerConfig.PeersetId,
			"config_peerId":                peerConfig.PeerId,
			"config_ratis_addresses":       ratisPeers,
			"config_peers":                 gpacPeers,
			"IS_METRIC_TEST":               strconv.FormatBool(isMetricTest),
			"config_persistence_type":      "REDIS",
			"config_persistence_redisHost": "localhost",
			"config_persistence_redisPort": "6379",
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

func createPV(namespace string, peerConfig utils.PeerConfig) {
	clientset, err := utils.GetClientset()
	if err != nil {
		panic(err)
	}

	hostPathType := apiv1.HostPathUnset

	pv := &apiv1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name:      utils.PVName(peerConfig, namespace),
			Namespace: namespace,
			Labels: map[string]string{
				"project": "ucac",
			},
		},
		Spec: apiv1.PersistentVolumeSpec{
			StorageClassName: "",
			Capacity: apiv1.ResourceList{
				"storage": resource.MustParse("50Mi"),
			},
			AccessModes: []apiv1.PersistentVolumeAccessMode{
				"ReadWriteOnce",
			},
			PersistentVolumeReclaimPolicy: apiv1.PersistentVolumeReclaimDelete,
			PersistentVolumeSource: apiv1.PersistentVolumeSource{
				HostPath: &apiv1.HostPathVolumeSource{
					Path: fmt.Sprintf("/mnt/ucac/redis/data-ucac-%s", utils.RandomString(6)),
					Type: &hostPathType,
				},
			},
		},
	}

	if _, err = clientset.CoreV1().PersistentVolumes().Create(context.Background(), pv, metav1.CreateOptions{}); err != nil {
		panic(err)
	}
}

func createPVC(namespace string, config utils.PeerConfig) {
	clientset, err := utils.GetClientset()
	if err != nil {
		panic(err)
	}

	storageClass := ""

	pvc := &apiv1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      utils.PVCName(config, namespace),
			Namespace: namespace,
			Labels: map[string]string{
				"project": "ucac",
			},
		},
		Spec: apiv1.PersistentVolumeClaimSpec{
			StorageClassName: &storageClass,
			AccessModes: []apiv1.PersistentVolumeAccessMode{
				"ReadWriteOnce",
			},
			Resources: apiv1.ResourceRequirements{
				Requests: apiv1.ResourceList{
					"storage": resource.MustParse("50Mi"),
				},
			},
		},
	}

	clientset.CoreV1().PersistentVolumeClaims(namespace).Create(context.Background(), pvc, metav1.CreateOptions{})
}

func createRedisConfigmap(namespace string, config utils.PeerConfig) {
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
			Name:      utils.RedisConfigmapName(config),
			Namespace: namespace,
			Labels: map[string]string{
				"project": "ucac",
			},
		},
		Data: map[string]string{
			"redis-config": "appendonly yes",
		},
	}

	clientset.CoreV1().ConfigMaps(namespace).Create(context.Background(), configMap, metav1.CreateOptions{})
}
