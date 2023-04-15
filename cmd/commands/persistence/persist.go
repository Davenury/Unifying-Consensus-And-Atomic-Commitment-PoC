package persistence

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/spf13/cobra"
	"log"
	"os"
	"os/exec"
	"time"
)

type FileInfo struct {
	Pod     string
	SrcPath string
	DstPath string
	Name    string
}

const (
	YYYYMMDD = "2006-01-02"
)

func getCurrentPath() string {
	path, err := os.Getwd()
	if err != nil {
		log.Println(err)
	}
	return path
}

func getDstPath(basePath string, experiment string) string {
	date := time.Now().UTC().Format(YYYYMMDD)

	return fmt.Sprintf("%s/%s/%s", basePath, date, experiment)
}

func persistSingle(info FileInfo) {
	cmd := exec.Command("kubectl", "cp", fmt.Sprintf("%s:%s", info.Pod, info.SrcPath), info.DstPath)
	err := cmd.Run()
	if err != nil {
		panic(err)
	}
}

func persist(basePath string, experiment string) {
	victoriaInfo := FileInfo{
		Pod:     "victoria-victoria-metrics-single-server-0",
		SrcPath: "/storage",
		DstPath: fmt.Sprintf("%s/victoria/storage", getDstPath(basePath, experiment)),
		Name:    "victoria",
	}

	lokiChunksInfo := FileInfo{
		Pod:     "loki-0",
		SrcPath: "/var/loki/chunks",
		DstPath: fmt.Sprintf("%s/loki/chunks", getDstPath(basePath, experiment)),
		Name:    "loki",
	}

	lokiWalInfo := FileInfo{
		Pod:     "loki-0",
		SrcPath: "/var/loki/wal",
		DstPath: fmt.Sprintf("%s/loki/wal", getDstPath(basePath, experiment)),
		Name:    "loki",
	}

	tempoTracesInfo := FileInfo{
		Pod:     "tempo-0",
		SrcPath: "/var/tempo/traces",
		DstPath: fmt.Sprintf("%s/tempo/traces", getDstPath(basePath, experiment)),
		Name:    "tempo",
	}

	tempoWalInfo := FileInfo{
		Pod:     "tempo-0",
		SrcPath: "/var/tempo/wal",
		DstPath: fmt.Sprintf("%s/tempo/wal", getDstPath(basePath, experiment)),
		Name:    "tempo",
	}

	for _, info := range []FileInfo{victoriaInfo, lokiChunksInfo, lokiWalInfo, tempoTracesInfo, tempoWalInfo} {
		persistSingle(info)
		fmt.Printf("Persisted: %s in %s\n", info.Name, info.DstPath)
	}
}

func CreatePersistCommand() *cobra.Command {

	var basePath string
	var experiment string

	var persistCommand = &cobra.Command{
		Use:   "persist",
		Short: "persist data from the experiment",
		Run: func(cmd *cobra.Command, args []string) {
			persist(basePath, experiment)
			fmt.Println(getCurrentPath())
		},
	}

	persistCommand.Flags().StringVar(&basePath, "basePath", getCurrentPath(), "Base path that results will be saved into")
	persistCommand.Flags().StringVar(&experiment, "experiment", uuid.New().String(), "Experiment uuid. Defaults to random uuid.")

	return persistCommand
}
