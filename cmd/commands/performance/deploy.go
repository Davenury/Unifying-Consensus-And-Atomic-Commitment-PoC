package performance

import (
	"context"
	"fmt"
	"github.com/spf13/cobra"
	"github.com/davenury/ucac/cmd/commands/utils"
	"strconv"

	batchv1 "k8s.io/api/batch/v1"
	"k8s.io/client-go/kubernetes"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

var performanceNamespace string
var performanceNumberOfPeers []int
var performanceImage string

var singleRequestsNumber int
var multipleRequestsNumber int
var testDuration string
var maxPeersetsInChange int
var testsStrategy string
var pushgatewayAddress string
var enforceAcUsage bool
var acProtocol string
var consensusProtocol string

func createPerformanceDeployCommand() *cobra.Command {

	var cmd = &cobra.Command{
		Use:   "deploy",
		Short: "Execute performance test",
		Run: func(cmd *cobra.Command, args []string) {

			clientset, err := utils.GetClientset()
			if err != nil {
				panic(err)
			}

			createService(clientset)
			createConfigmap(clientset)
			createJob(clientset, performanceImage)
		},
	}

	cmd.Flags().StringVarP(&performanceNamespace, "namespace", "n", "default", "Namespace to clear deployemtns for")
	cmd.Flags().IntSliceVar(&performanceNumberOfPeers, "peers", make([]int, 0), "Number of peers in peersets; example usage '--peers=1,2,3'")
	cmd.Flags().StringVarP(&performanceImage, "image", "", "ghcr.io/davenury/tests:latest", "Docker image for tests")

	cmd.Flags().IntVarP(&singleRequestsNumber, "single-requests-number", "", 1, "Determines number of requests to send to single peerset")
	cmd.Flags().IntVarP(&multipleRequestsNumber, "multiple-requests-number", "", 0, "Determines number of requests to send to multiple peersets at once")
	cmd.Flags().StringVarP(&testDuration, "test-duration", "d", "PT1S", "Duration of test (in java-like duration format)")
	cmd.Flags().IntVarP(&maxPeersetsInChange, "max-peersets-in-change", "", 2, "Determines maximum number of peersets that can take part in one change")
	cmd.Flags().StringVarP(&testsStrategy, "tests-strategy", "", "delay_on_conflicts", "Determines tests strategy - either random or delay_on_conflicts")
	cmd.Flags().StringVarP(&pushgatewayAddress, "pushgateway-address", "", "prometheus-prometheus-pushgateway.default:9091", "Pushgateway address")

	cmd.Flags().BoolVarP(&enforceAcUsage, "enforce-ac", "", false, "Determines if usage of AC protocol should be enforced even if it isn't required (GPAC)")
	cmd.Flags().StringVarP(&acProtocol, "ac-protocol", "", "gpac", "AC protocol to use in case it's needed. two_pc or gpac")
	cmd.Flags().StringVarP(&consensusProtocol, "consensus-protocol", "", "", "Consensus protocol to use. For now it's one protocol")

	return cmd
}

func createJob(clientset *kubernetes.Clientset, image string) {

	jobs := clientset.BatchV1().Jobs(performanceNamespace)

	jobSpec := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "performance-test",
			Namespace: performanceNamespace,
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
							Name:  "performance-test",
							Image: image,
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

func createConfigmap(clientset *kubernetes.Clientset) {
	configMap := &v1.ConfigMap{
		TypeMeta: metav1.TypeMeta{
			Kind:       "ConfigMap",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "performance-test-configmap",
			Namespace: performanceNamespace,
			Labels: map[string]string{
				"project":  "ucac",
				"app.name": "performanceTest",
			},
		},
		Data: map[string]string{
			"TEST_PEERS":                      utils.GenerateServicesForPeersStaticPort(performanceNumberOfPeers, 8080),
			"NOTIFICATION_SERVICE_ADDRESS":    "http://notification-service:8080",
			"SINGLE_PEERSET_CHANGES_NUMBER":   fmt.Sprintf("%d", singleRequestsNumber),
			"MULTIPLE_PEERSET_CHANGES_NUMBER": fmt.Sprintf("%d", multipleRequestsNumber),
			"TEST_DURATION":                   testDuration,
			"MAX_PEERSETS_IN_CHANGE":          fmt.Sprintf("%d", maxPeersetsInChange),
			"TESTS_STRATEGY":                  testsStrategy,
			"PUSHGATEWAY_ADDRESS":             pushgatewayAddress,
			"ENFORCE_AC_USAGE":                strconv.FormatBool(enforceAcUsage),
			"AC_PROTOCOL":					   acProtocol,
			"CONSENSUS_PROTOCOL":			   consensusProtocol,
		},
	}

	clientset.CoreV1().ConfigMaps(performanceNamespace).Create(context.Background(), configMap, metav1.CreateOptions{})
}

func createService(clientset *kubernetes.Clientset) {
	service := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "notification-service",
			Namespace: performanceNamespace,
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

	clientset.CoreV1().Services(performanceNamespace).Create(context.Background(), service, metav1.CreateOptions{})

}
