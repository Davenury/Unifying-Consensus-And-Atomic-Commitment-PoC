package performance

import (
	"context"
	"fmt"
	"github.com/davenury/ucac/cmd/commands/utils"
	"github.com/spf13/cobra"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type CheckConfig struct {
	namespace       string
	peersV1         []int
	peers           int
	peersets        string
	numberOfThreads int
	checkerImage    string
}

func (cfg *CheckConfig) getVersion() string {
	if cfg.peers != 0 {
		return "v2"
	}
	return "v1"
}

func (cfg *CheckConfig) GetPeersAndPeersets() (string, string) {
	if cfg.getVersion() == "v2" {
		peers, _ := utils.GenerateServicesForPeersStaticPort([]int{cfg.peers}, 8081)
		return peers, cfg.peersets
	}
	return utils.GenerateServicesForPeersStaticPort(cfg.peersV1, 8081)
}

func createCheckCommand() *cobra.Command {
	var config CheckConfig

	var cmd = &cobra.Command{
		Use:   "check",
		Short: "Deploys pod to check the changes in peers",
		Run: func(cmd *cobra.Command, args []string) {
			DoCheck(config)
		},
	}

	cmd.Flags().StringVarP(&config.namespace, "namespace", "n", "default", "Namespace to deploy checker")

	cmd.Flags().IntSliceVar(&config.peersV1, "peers", []int{}, "Peers v1")

	cmd.Flags().IntVar(&config.peers, "peers-v2", 0, "Number of peers passed to perform command")
	cmd.Flags().StringVar(&config.peersets, "peersets-v2", "", "Configuration of peersets passed to perform command")
	cmd.Flags().IntVar(&config.numberOfThreads, "number-of-threads", 1, "How many threads should be used in checking")
	cmd.Flags().StringVar(&config.checkerImage, "image", "ghcr.io/davenury/checker:latest", "Image")

	return cmd
}

func DoCheck(config CheckConfig) {
	clientset, err := utils.GetClientset()
	if err != nil {
		panic(err)
	}

	podsClient := clientset.CoreV1().Pods(config.namespace)

	peers, peersets := config.GetPeersAndPeersets()

	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "history-checker",
			Namespace: config.namespace,
			Labels: map[string]string{
				"project": "ucac",
			},
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name:  "history-checker",
					Image: config.checkerImage,
					Env: []v1.EnvVar{
						{
							Name:  "TEST_PEERS",
							Value: peers,
						},
						{
							Name:  "TEST_PEERSETS",
							Value: peersets,
						},
						{
							Name:  "NUMBER_OF_THREADS",
							Value: fmt.Sprintf("%d", config.numberOfThreads),
						},
						{
							Name:  "LOKI_BASE_URL",
							Value: "http://loki.ddebowski:3100",
						},
					},
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							"cpu":    resource.MustParse("1500m"),
							"memory": resource.MustParse("2Gi"),
						},
					},
				},
			},
			RestartPolicy: v1.RestartPolicyNever,
		},
	}

	podsClient.Create(context.Background(), pod, metav1.CreateOptions{})
}
