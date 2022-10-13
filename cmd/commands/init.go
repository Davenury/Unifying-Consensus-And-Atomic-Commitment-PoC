package commands

import (
	"fmt"
	"log"
	"github.com/spf13/cobra"
	"helm.sh/helm/v3/pkg/action"
	"helm.sh/helm/v3/pkg/cli"
	"helm.sh/helm/v3/pkg/chart/loader"
)

var settings *cli.EnvSettings
var initNamespace string
var initCreateNamespace bool

func CreateInitCommand() *cobra.Command {

	var initCommand = &cobra.Command{
		Use: "init",
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
		CreateNamespace(initNamespace)
	}

	// install prometheus
	installChart(initNamespace, "prometheus-community", "prometheus", "prometheus")

	// install grafana
	installChart(initNamespace, "grafana", "grafana", "grafana")
}

func installChart(namespace string, repoName string, chartName string, releaseName string) {
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
	release, err := client.Run(cr, nil)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Installed %s release to namespace %s", release.Name, namespace)
}