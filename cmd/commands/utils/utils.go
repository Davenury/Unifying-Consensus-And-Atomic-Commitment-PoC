package utils

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
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

func GetClientset() (*kubernetes.Clientset, error) {

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
	PeerId         string
	PeersetId      string
	PeersInPeerset int
	PeersetsConfig []int
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
	return fmt.Sprintf("peerset%s-peer%s-dep", peerConfig.PeersetId, peerConfig.PeerId)
}

func GenerateServicesForPeers(peersInPeerset []int, startPort int) string {
	return generateServicesForPeers(peersInPeerset, startPort, true)
}

func GenerateServicesForPeersStaticPort(peersInPeerset []int, port int) string {
	return generateServicesForPeers(peersInPeerset, port, false)
}

func ServiceAddress(peerConfig PeerConfig) string {
	return fmt.Sprintf("peer%s-peerset%s-service", peerConfig.PeerId, peerConfig.PeersetId)
}

func generateServicesForPeers(peersInPeerset []int, startPort int, increment bool) string {

	var resultSb strings.Builder
	for idx, peersNumber := range peersInPeerset {
		var sb strings.Builder

		for i := 0; i < peersNumber; i++ {
			port := startPort
			if increment {
				port = port + i
			}
			sb.WriteString(fmt.Sprintf("%s:%d,", ServiceAddress(PeerConfig{
				PeerId:    strconv.Itoa(i),
				PeersetId: strconv.Itoa(idx),
			}), port))
		}

		str := sb.String()

		if len(str) > 0 {
			str = str[:len(str)-1]
		}

		resultSb.WriteString(fmt.Sprintf("%s;", str))
	}

	result := resultSb.String()
	if len(result) > 0 {
		result = result[:len(result)-1]
	}

	return result
}

func CreateNamespace(namespaceName string) {
	clientset, err := GetClientset()

	if err != nil {
		panic(err)
	}

	nsList, err := clientset.CoreV1().Namespaces().List(context.Background(), metav1.ListOptions{})
	if err != nil {
		panic(err)
	}
	for _, ns := range nsList.Items {
		if ns.Name == namespaceName {
			return
		}
	}

	nsSpec := &apiv1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespaceName}}

	_, err = clientset.CoreV1().Namespaces().Create(context.Background(), nsSpec, metav1.CreateOptions{})

	if err != nil {
		panic(err)
	}
}
