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

type DeployConfig struct {
	NumberOfPeersInPeersets []int
	DeployNamespace         string
	DeployCreateNamespace   bool
	WaitForReadiness        bool
	ImageName               string
	IsMetricTest            bool
	CreateResources         bool
	ProxyDelay              string
	ProxyLimit              string
	MonitoringNamespace     string
	ConsensusAffinity       string
}

const ratisPort = 10024
const servicePort = 8080

func CreateDeployCommand() *cobra.Command {

	var config DeployConfig
	var deployCommand = &cobra.Command{
		Use:   "deploy",
		Short: "deploy application to cluster",
		Run: func(cmd *cobra.Command, args []string) {
			DoDeploy(config)
		},
	}

	deployCommand.Flags().IntSliceVar(&config.NumberOfPeersInPeersets, "peers", make([]int, 0), "Number of peers in peersets; example usage '--peers=1,2,3'")
	deployCommand.Flags().BoolVarP(&config.DeployCreateNamespace, "create-namespace", "", false, "Include if should create namespace")
	deployCommand.Flags().StringVarP(&config.DeployNamespace, "namespace", "n", "default", "Namespace to deploy cluster to")
	deployCommand.Flags().BoolVarP(&config.WaitForReadiness, "wait-for-readiness", "", false, "Wait for deployment to be ready")
	deployCommand.Flags().StringVarP(&config.ImageName, "image", "", "ghcr.io/davenury/ucac:latest", "A Docker image to be used in the deployment")
	deployCommand.Flags().BoolVar(&config.IsMetricTest, "is-metric-test", false, "Determines whether add additional change related metrics. DO NOT USE WITH NORMAL TESTS!")
	deployCommand.Flags().BoolVar(&config.CreateResources, "create-resources", true, "Determines if pods should have limits and requests")
	deployCommand.Flags().StringVar(&config.ProxyDelay, "proxy-delay", "0", "Delay in seconds for proxy, e.g. 0.2")
	deployCommand.Flags().StringVar(&config.ProxyLimit, "proxy-limit", "0", "Bandwidth limit in bytes per second, e.g. 100, 2M")
	deployCommand.Flags().StringVar(&config.MonitoringNamespace, "monitoring-namespace", "ddebowski", "Namespace with monitoring deployed")
	deployCommand.Flags().StringVar(&config.ConsensusAffinity, "consensus-affinity", "", "Consensus affinity = likeliness that peer will be consensus leader in peerset, e.g. peerset0=peer0;peerset1=peer0")

	return deployCommand

}

func DoDeploy(config DeployConfig) {
	peers, peersets := utils.GenerateServicesForPeersStaticPort(config.NumberOfPeersInPeersets, servicePort)
	ratisGroups := make([]string, len(config.NumberOfPeersInPeersets))
	for i := 0; i < len(config.NumberOfPeersInPeersets); i++ {
		ratisGroups[i] = uuid.New().String()
	}

	if config.DeployCreateNamespace {
		utils.CreateNamespace(config.DeployNamespace)
	}

	totalPeers := 0
	peerCount := 0
	for idx, num := range config.NumberOfPeersInPeersets {
		totalPeers += num
		for i := 0; i < num; i++ {
			peerId := fmt.Sprintf("peer%d", peerCount)
			peerCount += 1

			peerConfig := utils.PeerConfig{
				PeerId:         peerId,
				PeersetId:      strconv.Itoa(idx),
				PeersInPeerset: num,
				PeersetsConfig: config.NumberOfPeersInPeersets,
			}

			createPV(config.DeployNamespace, peerConfig)
			createPVC(config.DeployNamespace, peerConfig)
			createRedisConfigmap(config.DeployNamespace, peerConfig)

			deploySinglePeerService(config.DeployNamespace, peerConfig, ratisPort+i)

			deploySinglePeerConfigMap(config, peerConfig, peers, peersets)

			deploySinglePeerDeployment(config, peerConfig)

			fmt.Printf("Deployed app of peer %s from peerset %s\n", peerConfig.PeerId, peerConfig.PeersetId)
		}
	}

	if config.WaitForReadiness {
		waitForPodsReadiness(totalPeers, config.DeployNamespace)
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

func deploySinglePeerDeployment(config DeployConfig, peerConfig utils.PeerConfig) {
	clientset, err := utils.GetClientset()
	if err != nil {
		panic(err)
	}

	deploymentClient := clientset.AppsV1().Deployments(config.DeployNamespace)

	oneReplicaPointer := int32(1)

	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      utils.DeploymentName(peerConfig),
			Namespace: config.DeployNamespace,
			Labels: map[string]string{
				"project":   "ucac",
				"peerId":    peerConfig.PeerId,
				"peersetId": peerConfig.PeersetId,
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
			Template: createPodTemplate(config, peerConfig),
		},
	}

	fmt.Println("Creating deployment...")
	result, err := deploymentClient.Create(context.Background(), deployment, metav1.CreateOptions{})
	if err != nil {
		panic(err)
	}
	fmt.Printf("Created deployment: %q.\n", result.GetObjectMeta().GetName())

}

func createPodTemplate(config DeployConfig, peerConfig utils.PeerConfig) apiv1.PodTemplateSpec {
	containerName := utils.ContainerName(peerConfig)

	return apiv1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Name: containerName,
			Labels: map[string]string{
				"peerId":    peerConfig.PeerId,
				"peersetId": peerConfig.PeersetId,
				"app.name":  fmt.Sprintf("%s-app", peerConfig.PeerId),
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
				createSingleContainer(config, peerConfig),
				createProxyContainer(config.ProxyDelay, config.ProxyLimit),
				createRedisContainer(),
			},
			Volumes: []apiv1.Volume{
				{
					Name: "redis-data",
					VolumeSource: apiv1.VolumeSource{
						PersistentVolumeClaim: &apiv1.PersistentVolumeClaimVolumeSource{
							ClaimName: utils.PVCName(peerConfig, config.DeployNamespace),
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
func createSingleContainer(config DeployConfig, peerConfig utils.PeerConfig) apiv1.Container {

	probe := &apiv1.Probe{
		ProbeHandler: apiv1.ProbeHandler{
			HTTPGet: &apiv1.HTTPGetAction{
				Path: "/_meta/health",
				Port: intstr.FromInt(8081),
			},
		},
	}

	resources := apiv1.ResourceRequirements{}
	if config.CreateResources {
		resources = apiv1.ResourceRequirements{
			Limits: apiv1.ResourceList{
				"cpu":    resource.MustParse("1"),
				"memory": resource.MustParse("550Mi"),
			},
			Requests: apiv1.ResourceList{
				"cpu":    resource.MustParse("500m"),
				"memory": resource.MustParse("350Mi"),
			},
		}
	}

	return apiv1.Container{
		Name:      "application",
		Image:     config.ImageName,
		Resources: resources,
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

func deploySinglePeerConfigMap(config DeployConfig, peerConfig utils.PeerConfig, peers string, peersets string) {
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
			Namespace: config.DeployNamespace,
			Labels: map[string]string{
				"project":   "ucac",
				"peerId":    peerConfig.PeerId,
				"peersetId": peerConfig.PeersetId,
			},
		},
		Data: map[string]string{
			"CONFIG_FILE":                  "application-kubernetes.conf",
			"JAVA_OPTS":                    "-Xmx200m",
			"config_host":                  utils.ServiceAddress(peerConfig),
			"config_port":                  "8081",
			"config_peerId":                peerConfig.PeerId,
			"config_peers":                 peers,
			"config_peersets":              peersets,
			"IS_METRIC_TEST":               strconv.FormatBool(config.IsMetricTest),
			"config_persistence_type":      "REDIS",
			"config_persistence_redisHost": "localhost",
			"config_persistence_redisPort": "6379",
			"LOKI_BASE_URL":                fmt.Sprintf("http://loki.%s:3100", config.MonitoringNamespace),
			"NAMESPACE":                    config.DeployNamespace,
			"GPAC_FTAGREE_REPEAT_DELAY":    "PT0.5S",
			"CONSENSUS_AFFINITY":           config.ConsensusAffinity,
		},
	}

	clientset.CoreV1().ConfigMaps(config.DeployNamespace).Create(context.Background(), configMap, metav1.CreateOptions{})
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
				"project":   "ucac",
				"peerId":    peerConfig.PeerId,
				"peersetId": peerConfig.PeersetId,
			},
		},
		Spec: apiv1.ServiceSpec{
			Selector: map[string]string{
				"app.name": fmt.Sprintf("%s-app", peerConfig.PeerId),
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
				"project":   "ucac",
				"peerId":    peerConfig.PeerId,
				"peersetId": peerConfig.PeersetId,
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
				"project":   "ucac",
				"peerId":    config.PeerId,
				"peersetId": config.PeersetId,
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
				"project":   "ucac",
				"peerId":    config.PeerId,
				"peersetId": config.PeersetId,
			},
		},
		Data: map[string]string{
			"redis-config": "appendonly yes",
		},
	}

	clientset.CoreV1().ConfigMaps(namespace).Create(context.Background(), configMap, metav1.CreateOptions{})
}
