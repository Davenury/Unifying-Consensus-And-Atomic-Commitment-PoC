package commands

import (
	"fmt"
	"github.com/davenury/ucac/cmd/commands/utils"
	"github.com/spf13/cobra"
	"helm.sh/helm/v3/pkg/action"
	"helm.sh/helm/v3/pkg/chart/loader"
	"helm.sh/helm/v3/pkg/cli"
	"log"
)

var settings *cli.EnvSettings
var initNamespace string
var initCreateNamespace bool

func CreateInitCommand() *cobra.Command {

	var initCommand = &cobra.Command{
		Use:   "init",
		Short: "deploy prometheus and grafana to cluster",
		Run: func(cmd *cobra.Command, args []string) {
			initCommand()
		},
	}

	initCommand.Flags().StringVarP(&initNamespace, "namespace", "n", "monitoring", "Namespace to deploy cluster to")
	initCommand.Flags().BoolVarP(&initCreateNamespace, "create-namespace", "", false, "Include if passed namespace should be created")

	return initCommand
}

func initCommand() {
	settings = cli.New()

	// create namespace
	if initCreateNamespace {
		utils.CreateNamespace(initNamespace)
	}

	// install prometheus
	installChart(initNamespace, "prometheus-community", "prometheus", "prometheus", nil)

	grafanaValues := map[string]interface{}{
		"adminPassword": "admin123",
		"resources": map[string]interface{}{
			"limits": map[string]string{
				"cpu": "512m",
				"memory": "512Mi",
			},
			"requests": map[string]string{
				"cpu": "256m",
				"memory": "256Mi",
			},
		},
		"datasources": map[string]interface{}{
			"datasources.yaml": map[string]interface{}{
				"apiVersion": 1,
				"datasources": []map[string]interface{}{
					{
						"name": "Prometheus",
						"url": fmt.Sprintf("http://prometheus-server.%s.svc.cluster.local:80", initNamespace),
						"type": "prometheus",
						"isDefault": true,
					},
				},
			},
		},
		//"dashboards": map[string]interface{}{
		//	"default": map[string]interface{}{
		//		"ucac-dashboard": map[string]interface{}{
		//			"file": "./dashboard.json",
		//		},
		//	},
		//},
	}

	// install grafana
	installChart(initNamespace, "grafana", "grafana", "grafana", grafanaValues)
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
