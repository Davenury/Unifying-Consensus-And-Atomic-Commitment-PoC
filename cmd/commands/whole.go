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

func CreateWholeCommand() *cobra.Command {

	var config Config
	var cmd = &cobra.Command{
		Use:   "perform",
		Short: "whole command - deploys monitoring, peers, executes performance, cleanup",
		Run: func(cmd *cobra.Command, args []string) {
			perform(config)
		},
	}

	cmd.Flags().StringVar(&config.monitoringNamespace, "monitoring-namespace", "test", "Namespace to deploy monitoring to")
	cmd.Flags().BoolVar(&config.createMonitoringNamespace, "create-monitoring-namespace", false, "Include if passed monitoring namespace should be created")

	cmd.Flags().IntSliceVar(&config.numberOfPeersInPeersets, "peers", make([]int, 0), "Deprecated, does not support multiple peersets. Number of peers in peersets; example usage '--peers=1,2,3'")

	cmd.Flags().IntVar(&config.numberOfPeersV2, "peers-v2", 0, "Number of peers to deploy")
	cmd.Flags().StringVar(&config.peersetsConfigurationV2, "peersets-v2", "", "Peersets configuration, e.g. peerset1=peer0,peer1;peerset2=peer1,peer2")

	cmd.Flags().BoolVar(&config.createTestNamespace, "create-test-namespace", false, "Include if should create namespace")
	cmd.Flags().StringVar(&config.testNamespace, "test-namespace", "default", "Namespace to deploy cluster to")
	cmd.Flags().StringVar(&config.applicationImageName, "application-image", "ghcr.io/davenury/ucac:latest", "A Docker image to be used in the deployment")

	cmd.Flags().StringVar(&config.performanceImage, "performance-test-image", "ghcr.io/davenury/tests:latest", "Docker image for tests")
	cmd.Flags().IntVar(&config.singleRequestsNumber, "single-requests-number", 1, "Determines number of requests to send to single peerset")
	cmd.Flags().IntVar(&config.multipleRequestsNumber, "multiple-requests-number", 0, "Determines number of requests to send to multiple peersets at once")
	cmd.Flags().StringVar(&config.testDuration, "test-duration", "PT1S", "Duration of test (in java-like duration format)")
	cmd.Flags().IntVar(&config.maxPeersetsInChange, "max-peersets-in-change", 2, "Determines maximum number of peersets that can take part in one change")
	cmd.Flags().StringVar(&config.testsSendingStrategy, "tests-sending-strategy", "delay_on_conflicts", "Determines tests strategy - either random or delay_on_conflicts")
	cmd.Flags().StringVar(&config.testsCreatingChangeStrategy, "tests-creating-change-strategy", "default", "Determines creating changes strategy - either default or processable_conflicts")
	cmd.Flags().StringVar(&config.pushgatewayAddress, "pushgateway-address", "prometheus-prometheus-pushgateway:9091", "Pushgateway address")
	cmd.Flags().BoolVar(&config.enforceAcUsage, "enforce-ac", false, "Determines if usage of AC protocol should be enforced even if it isn't required (GPAC)")
	cmd.Flags().StringVar(&config.acProtocol, "ac-protocol", "gpac", "AC protocol to use in case it's needed. two_pc or gpac")
	cmd.Flags().StringVar(&config.consensusProtocol, "consensus-protocol", "", "Consensus protocol to use. For now it's one protocol")
	cmd.Flags().StringVar(&config.performanceTestTimeoutDeadline, "performance-test-timeout-deadline", "PT0S", "Additional duration after which test job should be force ended")
	cmd.Flags().BoolVar(&config.cleanupAfterWork, "cleanup-after-work", true, "Determines if command should clean ucac resources after work (doesn't appy to grafana and prometheus)")
	cmd.Flags().BoolVar(&config.isMetricTest, "is-metric-test", false, "Metric tests adds multiple metrics for changes per id. DON'T USE WITH NORMAL TESTS!")
	cmd.Flags().BoolVar(&config.deployMonitoring, "deploy-monitoring", true, "Determines whether deploy monitoring stack")
	cmd.Flags().StringVar(&config.constantLoad, "constant-load", "", "Number of changes per second for constant load - overrides test duration and number of changes")
	cmd.Flags().StringVar(&config.fixedPeersetsInChange, "fixed-peersets-in-change", "", "Determines fixed number of peersets in change. Overrides maxPeersetsInChange")
	cmd.Flags().StringVar(&config.proxyLimit, "proxy-limit", "0", "Bandwidth limit in bytes per second, e.g. 100, 2M")

	cmd.Flags().StringVar(&config.loadGeneratorType, "load-generator-type", "", "Load Generator Type - one of constant, bound or increasing")
	cmd.Flags().Float64Var(&config.increasingLoadBound, "load-bound", float64(100), "Bound of changes per second for increasing load generator")
	cmd.Flags().StringVar(&config.increasingLoadIncreaseDelay, "load-increase-delay", "PT60S", "Determines how long load should be constant before increasing")
	cmd.Flags().Float64Var(&config.increasingLoadIncreaseStep, "load-increase-step", float64(1), "Determines how much load should change after load-increase-delay time")
	cmd.Flags().BoolVar(&config.enforceConsensusLeader, "enforce-consensus-leader", true, "Determines if tests should always send changes to consensus leader")

	cmd.Flags().StringVar(&config.ProxyDelay.delayType, "delay-type", "", "Delay type, currently supported: fixed, random, peerset-fixed, peerset-random")
	cmd.Flags().StringVar(&config.ProxyDelay.fixedProxyDelay, "delay-fixed", "", "Fixed delay applied to all request; delay type - fixed")
	cmd.Flags().StringVar(&config.ProxyDelay.randomProxyDelayStart, "delay-random-start", "", "Start of the random range of delays, calculated with each request; delay type - random")
	cmd.Flags().StringVar(&config.ProxyDelay.randomProxyDelayEnd, "delay-random-end", "", "End of the random range of delays, calculated with each request; delay type - random")
	cmd.Flags().StringVar(&config.ProxyDelay.ownPeersetProxyDelay, "delay-own-peerset", "", "Delay applied to peers within the same peerset; delay type - peerset-fixed")
	cmd.Flags().StringVar(&config.ProxyDelay.otherPeersetProxyDelay, "delay-other-peerset", "", "Delay applied to peers outside of the peerset; delay type - peerset-fixed")
	cmd.Flags().StringVar(&config.ProxyDelay.ownPeersetRandomProxyDelayStart, "delay-own-peerset-start", "", "Start of the random range of delays applied to peers within the same peerset; delay type - peerset-random")
	cmd.Flags().StringVar(&config.ProxyDelay.ownPeersetRandomProxyDelayEnd, "delay-own-peerset-end", "", "End of the random range of delays applied to peers within the same peerset; delay type - peerset-random")
	cmd.Flags().StringVar(&config.ProxyDelay.otherPeersetRandomProxyDelayStart, "delay-other-peerset-start", "", "Start of the random range of delays applied to peers outside of the peerset; delay type - peerset-random")
	cmd.Flags().StringVar(&config.ProxyDelay.otherPeersetRandomProxyDelayEnd, "delay-other-peerset-end", "", "End of the random range of delays applied to peers outside of the peerset; delay type - peerset-random")

	return cmd
}

func perform(config Config) {
	if config.deployMonitoring {
		fmt.Println("Deploying monitoring...")
		DoInit(config.monitoringNamespace, config.createMonitoringNamespace, false)
	}
	fmt.Println("Deploying application...")
	DoDeploy(DeployConfig{
		NumberOfPeersInPeersets: config.numberOfPeersInPeersets,
		NumberOfPeersV2:         config.numberOfPeersV2,
		PeersetsConfigurationV2: config.peersetsConfigurationV2,
		DeployNamespace:         config.testNamespace,
		DeployCreateNamespace:   config.createTestNamespace,
		WaitForReadiness:        true,
		ImageName:               config.applicationImageName,
		IsMetricTest:            config.isMetricTest,
		CreateResources:         true,
		ProxyDelay:              config.ProxyDelay,
		ProxyLimit:              config.proxyLimit,
		MonitoringNamespace:     config.monitoringNamespace,
	})
	fmt.Println("Delay for peersets to be ready e.g. select consensus leader")
	time.Sleep(30 * time.Second)
	fmt.Println("Deploying performance test")
	performance.DoPerformanceTest(performance.Config{
		PerformanceNamespace:        config.testNamespace,
		PerformanceNumberOfPeers:    config.numberOfPeersInPeersets,
		PerformanceImage:            config.performanceImage,
		SingleRequestsNumber:        config.singleRequestsNumber,
		MultipleRequestsNumber:      config.multipleRequestsNumber,
		MaxPeersetsInChange:         config.maxPeersetsInChange,
		TestsSendingStrategy:        config.testsSendingStrategy,
		TestsCreatingChangeStrategy: config.testsCreatingChangeStrategy,
		PushgatewayAddress:          config.pushgatewayAddress,
		EnforceAcUsage:              config.enforceAcUsage,
		AcProtocol:                  config.acProtocol,
		ConsensusProtocol:           config.consensusProtocol,
		FixedPeersetsInChange:       config.fixedPeersetsInChange,
		MonitoringNamespace:         config.monitoringNamespace,

		LoadGeneratorType:           config.loadGeneratorType,
		ConstantLoad:                config.constantLoad,
		TestDuration:                config.testDuration,
		IncreasingLoadBound:         config.increasingLoadBound,
		IncreasingLoadIncreaseDelay: config.increasingLoadIncreaseDelay,
		IncreasingLoadIncreaseStep:  config.increasingLoadIncreaseStep,
		EnforceConsensusLeader:      config.enforceConsensusLeader,
	})
	fmt.Println("Waiting for test to finish. You can Ctrl+C now, if you don't want to wait for the result. YOU SHOULD CLEANUP AFTER YOURSELF!")
	waitUntilJobPodCompleted(config)
	if config.cleanupAfterWork {
		fmt.Println("Cleanuping")
		DoCleanup(config.testNamespace)
		fmt.Printf("Do cleanup after monitoring after you're done with it by: helm delete prometheus grafana -n %s \n", config.monitoringNamespace)
	}
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
