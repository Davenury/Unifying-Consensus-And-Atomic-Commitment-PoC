package utils

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"time"

	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
)

func RandomString(length int) string {
	rand.Seed(time.Now().UnixNano())
	b := make([]byte, length+2)
	rand.Read(b)
	return fmt.Sprintf("%x", b)[2 : length+2]
}

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
	return fmt.Sprintf("%s-config", peerConfig.PeerId)
}

func ContainerName(peerConfig PeerConfig) string {
	return fmt.Sprintf("%s", peerConfig.PeerId)
}

func ServiceName(peerConfig PeerConfig) string {
	return fmt.Sprintf("%s-service", peerConfig.PeerId)
}

func DeploymentName(peerConfig PeerConfig) string {
	return fmt.Sprintf("%s-dep", peerConfig.PeerId)
}

func GenerateServicesForPeersStaticPort(peersInPeerset []int, port int) (string, string) {
	return generateServicesForPeers(peersInPeerset, port)
}

func ServiceAddress(peerConfig PeerConfig) string {
	return fmt.Sprintf("%s-service", peerConfig.PeerId)
}

func PVName(config PeerConfig, namespace string) string {
	return fmt.Sprintf("%s-%s-pv", namespace, config.PeerId)
}
func PVCName(config PeerConfig, namespace string) string {
	return fmt.Sprintf("%s-%s-claim", namespace, config.PeerId)
}
func RedisConfigmapName(config PeerConfig) string {
	return fmt.Sprintf("%s-rediscf", config.PeerId)
}

func generateServicesForPeers(peersInPeerset []int, startPort int) (string, string) {
	var peersSb strings.Builder
	var peersetsSb strings.Builder
	peerCount := 0
	for idx, peersNumber := range peersInPeerset {
		var sb strings.Builder
		var sb2 strings.Builder

		for i := 0; i < peersNumber; i++ {
			port := startPort
			sb.WriteString(fmt.Sprintf("peer%d=%s:%d;", peerCount, ServiceAddress(PeerConfig{
				PeerId:    fmt.Sprintf("peer%d", peerCount),
				PeersetId: strconv.Itoa(idx),
			}), port))
			sb2.WriteString(fmt.Sprintf("peer%d,", peerCount))
			peerCount += 1
		}

		str := sb.String()
		str2 := sb2.String()

		if len(str) > 0 {
			str = str[:len(str)-1]
		}
		if len(str2) > 0 {
			str2 = str2[:len(str2)-1]
		}

		peersSb.WriteString(fmt.Sprintf("%s;", str))
		peersetsSb.WriteString(fmt.Sprintf("peerset%d=%s;", idx, str2))
	}

	peers := peersSb.String()
	if len(peers) > 0 {
		peers = peers[:len(peers)-1]
	}
	peersets := peersetsSb.String()
	if len(peersets) > 0 {
		peersets = peersets[:len(peersets)-1]
	}

	return peers, peersets
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
