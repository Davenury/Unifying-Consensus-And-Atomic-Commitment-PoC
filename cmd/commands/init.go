package commands

import (
	"fmt"
	"github.com/davenury/ucac/cmd/commands/utils"
	"github.com/spf13/cobra"
	"helm.sh/helm/v3/pkg/action"
	"helm.sh/helm/v3/pkg/chart/loader"
	"helm.sh/helm/v3/pkg/cli"
	"log"
	"os"
)

var settings *cli.EnvSettings

func CreateInitCommand() *cobra.Command {
	var initNamespace string
	var initCreateNamespace bool

	var initCommand = &cobra.Command{
		Use:   "init",
		Short: "deploy prometheus and grafana to cluster",
		Run: func(cmd *cobra.Command, args []string) {
			DoInit(initNamespace, initCreateNamespace)
		},
	}

	initCommand.Flags().StringVarP(&initNamespace, "namespace", "n", "monitoring", "Namespace to deploy cluster to")
	initCommand.Flags().BoolVarP(&initCreateNamespace, "create-namespace", "", false, "Include if passed namespace should be created")

	return initCommand
}

func DoInit(namespace string, createNamespace bool) {
	settings = cli.New()

	// create namespace
	if createNamespace {
		utils.CreateNamespace(namespace)
	}

	victoriaValues := map[string]interface{}{
		"server": map[string]interface{}{
			"scrape": map[string]interface{}{
				"enabled": true,
			},
			"persistentVolume": map[string]interface{}{
				"enabled": false,
			},
		},
	}
	installChart(namespace, "vm", "victoria-metrics-single", "victoria", victoriaValues)

	grafanaValues := map[string]interface{}{
		"adminPassword": "admin123",
		"resources": map[string]interface{}{
			"limits": map[string]string{
				"cpu":    "512m",
				"memory": "512Mi",
			},
			"requests": map[string]string{
				"cpu":    "256m",
				"memory": "256Mi",
			},
		},
		"grafana.ini": map[string]interface{}{
			"auth.anonymous": map[string]interface{}{
				"enabled":  true,
				"org_role": "Admin",
			},
		},
		"datasources": map[string]interface{}{
			"datasources.yaml": map[string]interface{}{
				"apiVersion": 1,
				"datasources": []map[string]interface{}{
					{
						"name":      "VictoriaMetrics",
						"uid":       "PBFA97CFB590B2093",
						"url":       fmt.Sprintf("http://victoria-victoria-metrics-single-server.%s:8428/", namespace),
						"type":      "prometheus",
						"isDefault": true,
					},
					{
						"name": "Loki",
						"url":  fmt.Sprintf("http://loki.%s:3100", namespace),
						"type": "loki",
					},
					{
						"name": "Tempo",
						"url":  fmt.Sprintf("http://tempo.%s:3100", namespace),
						"type": "tempo",
						"jsonData": map[string]interface{}{
							"serviceMap": map[string]interface{}{
								"datasourceUid": "PBFA97CFB590B2093",
							},
						},
					},
				},
			},
		},
		"dashboardProviders": map[string]interface{}{
			"dashboardproviders.yaml": map[string]interface{}{
				"apiVersion": 1,
				"providers": []map[string]interface{}{
					{
						"name":            "default",
						"orgId":           1,
						"folder":          "",
						"type":            "file",
						"disableDeletion": "false",
						"editable":        true,
						"options": map[string]string{
							"path": "/var/lib/grafana/dashboards/default",
						},
					},
				},
			},
		},
		"dashboards": map[string]interface{}{
			"default": map[string]interface{}{
				"kubernetes": map[string]interface{}{
					"json": readFile("dashboard.json"),
				},
			},
		},
	}

	// install grafana
	installChart(namespace, "grafana", "grafana", "grafana", grafanaValues)

	lokiValues := map[string]interface{}{
		"loki": map[string]interface{}{
			"commonConfig": map[string]interface{}{
				"replication_factor": 1,
			},
			"storage": map[string]interface{}{
				"type": "filesystem",
			},
			"auth_enabled": false,
			"server": map[string]interface{}{
				"grpc_server_max_recv_msg_size": 6500000,
				"grpc_server_max_send_msg_size": 6500000,
			},
		},
		"singleBinary": map[string]interface{}{
			"replicas": 1,
		},
		"test": map[string]interface{}{
			"enabled": false,
		},
		"gateway": map[string]interface{}{
			"enabled": false,
		},
		"monitoring": map[string]interface{}{
			"selfMonitoring": map[string]interface{}{
				"grafanaAgent": map[string]interface{}{
					"installOperator": false,
				},
			},
			"lokiCanary": map[string]interface{}{
				"enabled": false,
			},
		},
	}

	installChart(namespace, "grafana", "loki", "loki", lokiValues)

	tempoValues := map[string]interface{}{
		"tempo": map[string]interface{}{
			"metricsGenerator": map[string]interface{}{
				"enabled":        true,
				"remoteWriteUrl": fmt.Sprintf("http://victoria-victoria-metrics-single-server.%s:8428/api/v1/write", namespace),
			},
			"reportingEnabled": false,
			"global_overrides": map[string]interface{}{
				"max_traces_per_user": 0,
				"metrics_generator_processor_span_metrics_dimensions": []string{"experiment"},
			},
		},
	}

	installChart(namespace, "grafana", "tempo", "tempo", tempoValues)
}

func installChart(namespace string, repoName string, chartName string, releaseName string, values map[string]interface{}) {
	actionConfig := new(action.Configuration)
	if err := actionConfig.Init(settings.RESTClientGetter(), namespace, "", log.Printf); err != nil {
		log.Fatal(err)
	}
	client := action.NewInstall(actionConfig)

	client.ReleaseName = releaseName
	cp, err := client.ChartPathOptions.LocateChart(fmt.Sprintf("%s/%s", repoName, chartName), settings)
	if err != nil {
		log.Fatal(err)
	}

	cr, err := loader.Load(cp)
	if err != nil {
		log.Fatal(err)
	}

	client.Namespace = namespace
	release, err := client.Run(cr, values)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Installed %s release to namespace %s\n", release.Name, namespace)
}

func readFile(path string) string {
	data, err := os.ReadFile(path)
	if err != nil {
		panic(err)
	}

	return string(data)
}
