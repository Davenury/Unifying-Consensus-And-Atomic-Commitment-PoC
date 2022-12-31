package commands

import (
	"context"
	"fmt"
	"github.com/davenury/ucac/cmd/commands/performance"
	"github.com/davenury/ucac/cmd/commands/utils"
	"github.com/senseyeio/duration"
	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"time"
)

type Config struct {
	monitoringNamespace string
	createMonitoringNamespace bool
	numberOfPeersInPeersets []int
	createTestNamespace bool
	testNamespace string
	applicationImageName string
	performanceImage string
	singleRequestsNumber int
	multipleRequestsNumber int
	testDuration string
	maxPeersetsInChange int
	testsStrategy string
	pushgatewayAddress string
	enforceAcUsage bool
	acProtocol string
	consensusProtocol string
	performanceTestTimeoutDeadline string
}

func CreateWholeCommand() *cobra.Command {

	var config Config
	var cmd = &cobra.Command{
		Use: "perform",
		Short: "whole command - deploys monitoring, peers, executes performance, cleanup",
		Run: func(cmd *cobra.Command, args []string) {
			perform(config)
		},
	}

	cmd.Flags().StringVar(&config.monitoringNamespace, "monitoring-namespace", "test", "Namespace to deploy monitoring to")
	cmd.Flags().BoolVar(&config.createMonitoringNamespace, "create-monitoring-namespace", false, "Include if passed monitoring namespace should be created")

	cmd.Flags().IntSliceVar(&config.numberOfPeersInPeersets, "peers", make([]int, 0), "Number of peers in peersets; example usage '--peers=1,2,3'")
	cmd.Flags().BoolVar(&config.createTestNamespace, "create-test-namespace", false, "Include if should create namespace")
	cmd.Flags().StringVar(&config.testNamespace, "test-namespace", "default", "Namespace to deploy cluster to")
	cmd.Flags().StringVar(&config.applicationImageName, "application-image", "ghcr.io/davenury/ucac:latest", "A Docker image to be used in the deployment")

	cmd.Flags().StringVar(&config.performanceImage, "performance-test-image", "ghcr.io/davenury/tests:latest", "Docker image for tests")
	cmd.Flags().IntVar(&config.singleRequestsNumber, "single-requests-number", 1, "Determines number of requests to send to single peerset")
	cmd.Flags().IntVar(&config.multipleRequestsNumber, "multiple-requests-number", 0, "Determines number of requests to send to multiple peersets at once")
	cmd.Flags().StringVar(&config.testDuration, "test-duration", "PT1S", "Duration of test (in java-like duration format)")
	cmd.Flags().IntVar(&config.maxPeersetsInChange, "max-peersets-in-change", 2, "Determines maximum number of peersets that can take part in one change")
	cmd.Flags().StringVar(&config.testsStrategy, "tests-strategy", "delay_on_conflicts", "Determines tests strategy - either random or delay_on_conflicts")
	cmd.Flags().StringVar(&config.pushgatewayAddress, "pushgateway-address", "prometheus-prometheus-pushgateway.default:9091", "Pushgateway address")
	cmd.Flags().BoolVar(&config.enforceAcUsage, "enforce-ac", false, "Determines if usage of AC protocol should be enforced even if it isn't required (GPAC)")
	cmd.Flags().StringVar(&config.acProtocol, "ac-protocol", "gpac", "AC protocol to use in case it's needed. two_pc or gpac")
	cmd.Flags().StringVar(&config.consensusProtocol, "consensus-protocol", "", "Consensus protocol to use. For now it's one protocol")
	cmd.Flags().StringVar(&config.performanceTestTimeoutDeadline, "performance-test-timeout-deadline", "PT0S", "Additional duration after which test job should be force ended")

	return cmd
}

func perform(config Config) {
	fmt.Println("Deploying monitoring...")
	DoInit(config.monitoringNamespace, config.createMonitoringNamespace)
	fmt.Println("Deploying application...")
	DoDeploy(config.numberOfPeersInPeersets, config.createTestNamespace, config.testNamespace, true, config.applicationImageName)
	fmt.Println("Delay for peersets to be ready e.g. select consensus leader")
	time.Sleep(10 * time.Second)
	fmt.Println("Deploying performance test")
	performance.DoPerformanceTest(performance.Config{
		PerformanceNamespace: config.testNamespace,
		PerformanceNumberOfPeers: config.numberOfPeersInPeersets,
		PerformanceImage: config.performanceImage,
		SingleRequestsNumber: config.singleRequestsNumber,
		MultipleRequestsNumber: config.multipleRequestsNumber,
		TestDuration: config.testDuration,
		MaxPeersetsInChange: config.maxPeersetsInChange,
		TestsStrategy: config.testsStrategy,
		PushgatewayAddress: config.pushgatewayAddress,
		EnforceAcUsage: config.enforceAcUsage,
		AcProtocol: config.acProtocol,
		ConsensusProtocol: config.consensusProtocol,
	})
	fmt.Println("Waiting for test to finish. You can Ctrl+C now, if you don't want to wait for the result. YOU SHOULD CLEANUP AFTER YOURSELF!")
	waitUntilJobPodCompleted(config)
	fmt.Println("Cleanuping")
	DoCleanup(config.testNamespace)
	fmt.Printf("Do cleanup after monitoring after you're done with it by: helm delete prometheus grafana -n %s \n", config.monitoringNamespace)
}

func waitUntilJobPodCompleted(config Config) {
	clientset, err := utils.GetClientset()
	if err != nil {
		panic(err)
	}

	now := time.Now()
	testDuration, err := duration.ParseISO8601(config.testDuration)
	if err != nil {
		panic(err)
	}
	additionalDeadline, err := duration.ParseISO8601(config.performanceTestTimeoutDeadline)
	if err != nil {
		panic(err)
	}

	deadline := testDuration.Shift(now)
	deadline = additionalDeadline.Shift(deadline)

	for !jobPodCompleted(config.testNamespace, clientset) {
		if time.Now().After(deadline) {
			panic("Timed out while waiting for pod readiness")
		}
		time.Sleep(1 * time.Second)
	}
}

func jobPodCompleted(namespace string, clientset *kubernetes.Clientset) bool {
	pods, _ := clientset.CoreV1().Pods(namespace).List(context.Background(), metav1.ListOptions{
		LabelSelector: "app.name=performance-test",
	})

	if len(pods.Items) == 0 {
		return false
	}

	jobPod := pods.Items[0]
	return jobPod.Status.Phase == "Succeeded"
}
