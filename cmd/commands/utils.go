package commands


import (
	"context"
	"fmt"
	"errors"
	"strconv"
	"strings"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func GetKubernetesConfig() (*rest.Config, error) {
	home := homedir.HomeDir()
	if home == "" {
		fmt.Println("Cannot get home directory")
		return nil, errors.New("Cannot get home directory")
	}
	
	var config *rest.Config
		_ = config

	return clientcmd.BuildConfigFromFlags("", fmt.Sprintf("%s/.kube/config", home))
}

func GetClientset() (*kubernetes.Clientset, error)  {
	
	config, err := GetKubernetesConfig()
	if err != nil {
		fmt.Printf("Error occured while trying to get kubeconfig: %s", err)
		return nil, err
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		fmt.Printf("Error occured while trying to create clientset: %s", err)
		return nil, err
	}

	return clientset, nil
}

type PeerConfig struct {
	PeerId string
	PeersetId string
	PeersInPeerset int
}

func ConfigMapName(peerConfig PeerConfig) string {
	return fmt.Sprintf("peer%s-peerset%s-config", peerConfig.PeerId, peerConfig.PeersetId)
}

func ContainerName(peerConfig PeerConfig) string {
	return fmt.Sprintf("peer%s-from-peerset%s", peerConfig.PeerId, peerConfig.PeersetId)
}

func ServiceName(peerConfig PeerConfig) string {
	return fmt.Sprintf("peer%s-peerset%s-service", peerConfig.PeerId, peerConfig.PeersetId)
}

func DeploymentName(peerConfig PeerConfig) string {
	return fmt.Sprintf("peer%s-peerset%s-dep", peerConfig.PeerId, peerConfig.PeersetId)
}

func GenerateServicesForPeers(peerConfig PeerConfig, startPort int) string {
	return generateServicesForPeers(peerConfig, startPort, true)
}

func GenerateServicesForPeersStaticPort(peerConfig PeerConfig, port int) string {
	return generateServicesForPeers(peerConfig, port, false)
}

func generateServicesForPeers(peerConfig PeerConfig, startPort int, increment bool) string {
	var sb strings.Builder

	for i := 1; i <= peerConfig.PeersInPeerset; i++ {
		port := startPort
		if increment {
			port = port + i
		}
		sb.WriteString(fmt.Sprintf("\"%s:%d\",", ServiceName(PeerConfig{
			PeerId: strconv.Itoa(i),
			PeersetId: peerConfig.PeersetId,
			PeersInPeerset: peerConfig.PeersInPeerset,
		}), port))
	}
	
	str := sb.String()

	if len(str) > 0 {
		str = str[:len(str)-1]
	}

	return str
}

func CreateNamespace(namespaceName string) {
	clientset, err := GetClientset()

	if err != nil {
		panic(err)
	}

	nsSpec := &apiv1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespaceName}}

	_, err = clientset.CoreV1().Namespaces().Create(context.Background(), nsSpec, metav1.CreateOptions{})

	if err != nil {
		panic(err)
	}
}