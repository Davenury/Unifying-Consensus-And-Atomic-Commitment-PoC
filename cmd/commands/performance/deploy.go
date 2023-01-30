package performance

import (
	"context"
	"fmt"
	"github.com/davenury/ucac/cmd/commands/utils"
	"github.com/spf13/cobra"
	"strconv"

	batchv1 "k8s.io/api/batch/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/client-go/kubernetes"
)

type Config struct {
	PerformanceNamespace        string
	PerformanceNumberOfPeers    []int
	PerformanceImage            string
	SingleRequestsNumber        int
	MultipleRequestsNumber      int
	TestDuration                string
	MaxPeersetsInChange         int
	TestsSendingStrategy        string
	TestsCreatingChangeStrategy string
	PushgatewayAddress          string
	EnforceAcUsage              bool
	AcProtocol                  string
	ConsensusProtocol           string
	ConstantLoad                string
	FixedPeersetsInChange       string
}

func createPerformanceDeployCommand() *cobra.Command {

	var performanceNamespace string
	var performanceNumberOfPeers []int
	var performanceImage string

	var singleRequestsNumber int
	var multipleRequestsNumber int
	var testDuration string
	var maxPeersetsInChange int
	var testsStrategy string
	var testsCreatingChangeStrategy string
	var pushgatewayAddress string
	var enforceAcUsage bool
	var acProtocol string
	var consensusProtocol string
	var constantLoad string
	var fixedPeersetsInChange string

	var cmd = &cobra.Command{
		Use:   "deploy",
		Short: "Execute performance test",
		Run: func(cmd *cobra.Command, args []string) {
			DoPerformanceTest(Config{
				PerformanceNamespace:        performanceNamespace,
				PerformanceNumberOfPeers:    performanceNumberOfPeers,
				PerformanceImage:            performanceImage,
				SingleRequestsNumber:        singleRequestsNumber,
				MultipleRequestsNumber:      multipleRequestsNumber,
				TestDuration:                testDuration,
				MaxPeersetsInChange:         maxPeersetsInChange,
				TestsSendingStrategy:        testsStrategy,
				TestsCreatingChangeStrategy: testsCreatingChangeStrategy,
				PushgatewayAddress:          pushgatewayAddress,
				EnforceAcUsage:              enforceAcUsage,
				AcProtocol:                  acProtocol,
				ConsensusProtocol:           consensusProtocol,
				ConstantLoad:                constantLoad,
				FixedPeersetsInChange:       fixedPeersetsInChange,
			})
		},
	}

	cmd.Flags().StringVarP(&performanceNamespace, "namespace", "n", "default", "Namespace to clear deployemtns for")
	cmd.Flags().IntSliceVar(&performanceNumberOfPeers, "peers", make([]int, 0), "Number of peers in peersets; example usage '--peers=1,2,3'")
	cmd.Flags().StringVarP(&performanceImage, "image", "", "ghcr.io/davenury/tests:latest", "Docker image for tests")

	cmd.Flags().IntVarP(&singleRequestsNumber, "single-requests-number", "", 1, "Determines number of requests to send to single peerset")
	cmd.Flags().IntVarP(&multipleRequestsNumber, "multiple-requests-number", "", 0, "Determines number of requests to send to multiple peersets at once")
	cmd.Flags().StringVarP(&testDuration, "test-duration", "d", "PT1S", "Duration of test (in java-like duration format)")
	cmd.Flags().IntVarP(&maxPeersetsInChange, "max-peersets-in-change", "", 2, "Determines maximum number of peersets that can take part in one change")
	cmd.Flags().StringVarP(&testsStrategy, "tests-sending-strategy", "", "delay_on_conflicts", "Determines tests strategy - either random or delay_on_conflicts")
	cmd.Flags().StringVarP(&testsCreatingChangeStrategy, "tests-creating-change-strategy", "", "delay_on_conflicts", "Determines tests strategy - either random or delay_on_conflicts")
	cmd.Flags().StringVarP(&pushgatewayAddress, "pushgateway-address", "", "prometheus-prometheus-pushgateway.default:9091", "Pushgateway address")

	cmd.Flags().BoolVarP(&enforceAcUsage, "enforce-ac", "", false, "Determines if usage of AC protocol should be enforced even if it isn't required (GPAC)")
	cmd.Flags().StringVarP(&acProtocol, "ac-protocol", "", "gpac", "AC protocol to use in case it's needed. two_pc or gpac")
	cmd.Flags().StringVarP(&consensusProtocol, "consensus-protocol", "", "", "Consensus protocol to use. For now it's one protocol")
	cmd.Flags().StringVar(&constantLoad, "constant-load", "", "Number of changes per second for constant load - overrides test duration and number of changes")
	cmd.Flags().StringVar(&fixedPeersetsInChange, "fixed-peersets-in-change", "", "Determines fixed number of peersets in change. Overrides maxPeersetsInChange")

	return cmd
}

func DoPerformanceTest(config Config) {
	clientset, err := utils.GetClientset()
	if err != nil {
		panic(err)
	}

	createService(clientset, config)
	createConfigmap(clientset, config)
	createJob(clientset, config)
}

func createJob(clientset *kubernetes.Clientset, config Config) {

	jobs := clientset.BatchV1().Jobs(config.PerformanceNamespace)

	jobSpec := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "performance-test",
			Namespace: config.PerformanceNamespace,
			Labels: map[string]string{
				"project": "ucac",
			},
		},
		Spec: batchv1.JobSpec{
			Template: v1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Name: "performance-test",
					Labels: map[string]string{
						"app.name": "performance-test",
						"project":  "ucac",
					},
					Annotations: map[string]string{
						"prometheus.io/scrape": "true",
						"prometheus.io/port":   "8080",
						"prometheus.io/path":   "/_meta/metrics",
					},
				},
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name: "performance-test",
							Resources: v1.ResourceRequirements{
								Limits: v1.ResourceList{
									"cpu":    resource.MustParse("400m"),
									"memory": resource.MustParse("800Mi"),
								},
								Requests: v1.ResourceList{
									"cpu":    resource.MustParse("200m"),
									"memory": resource.MustParse("400Mi"),
								},
							},
							Image: config.PerformanceImage,
							Ports: []v1.ContainerPort{
								{
									ContainerPort: 8080,
								},
							},
							EnvFrom: []v1.EnvFromSource{
								{
									ConfigMapRef: &v1.ConfigMapEnvSource{
										LocalObjectReference: v1.LocalObjectReference{
											Name: "performance-test-configmap",
										},
									},
								},
							},
						},
					},
					RestartPolicy: v1.RestartPolicyNever,
				},
			},
		},
	}

	_, err := jobs.Create(context.Background(), jobSpec, metav1.CreateOptions{})
	if err != nil {
		panic(err)
	}
}

func createConfigmap(clientset *kubernetes.Clientset, config Config) {

	data := map[string]string{
		"TEST_PEERS":                      utils.GenerateServicesForPeersStaticPort(config.PerformanceNumberOfPeers, 8080),
		"NOTIFICATION_SERVICE_ADDRESS":    "http://notification-service:8080",
		"SINGLE_PEERSET_CHANGES_NUMBER":   fmt.Sprintf("%d", config.SingleRequestsNumber),
		"MULTIPLE_PEERSET_CHANGES_NUMBER": fmt.Sprintf("%d", config.MultipleRequestsNumber),
		"TEST_DURATION":                   config.TestDuration,
		"MAX_PEERSETS_IN_CHANGE":          fmt.Sprintf("%d", config.MaxPeersetsInChange),
		"TESTS_SENDING_STRATEGY":          config.TestsSendingStrategy,
		"TESTS_CREATING_CHANGES_STRATEGY": config.TestsCreatingChangeStrategy,
		"PUSHGATEWAY_ADDRESS":             config.PushgatewayAddress,
		"ENFORCE_AC_USAGE":                strconv.FormatBool(config.EnforceAcUsage),
		"AC_PROTOCOL":                     config.AcProtocol,
		"CONSENSUS_PROTOCOL":              config.ConsensusProtocol,
	}

	if config.FixedPeersetsInChange != "" {
		data["FIXED_PEERSETS_IN_CHANGE"] = config.FixedPeersetsInChange
	}
	if config.ConstantLoad != "" {
		data["CONSTANT_LOAD"] = config.ConstantLoad
	}

	configMap := &v1.ConfigMap{
		TypeMeta: metav1.TypeMeta{
			Kind:       "ConfigMap",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "performance-test-configmap",
			Namespace: config.PerformanceNamespace,
			Labels: map[string]string{
				"project":  "ucac",
				"app.name": "performanceTest",
			},
		},
		Data: data,
	}

	clientset.CoreV1().ConfigMaps(config.PerformanceNamespace).Create(context.Background(), configMap, metav1.CreateOptions{})
}

func createService(clientset *kubernetes.Clientset, config Config) {
	service := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "notification-service",
			Namespace: config.PerformanceNamespace,
			Labels: map[string]string{
				"project": "ucac",
			},
		},
		Spec: v1.ServiceSpec{
			Selector: map[string]string{
				"app.name": "performance-test",
			},
			Ports: []v1.ServicePort{
				{
					Name:       "service",
					Port:       8080,
					TargetPort: intstr.FromInt(8080),
				},
			},
		},
	}

	clientset.CoreV1().Services(config.PerformanceNamespace).Create(context.Background(), service, metav1.CreateOptions{})

}
