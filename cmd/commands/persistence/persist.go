package persistence

import (
	"fmt"
	"github.com/spf13/cobra"
	"os/exec"
)

func persistSingle(pod string, srcPath string, dstPath string) {
	cmd := exec.Command("kubectl", "cp", fmt.Sprintf("%s:%s", pod, srcPath), dstPath)
	err := cmd.Run()
	if err != nil {
		panic(err)
	}
}

func persist() {
	
}

func CreatePersistCommand() *cobra.Command {

	var persistCommand = &cobra.Command{
		Use:   "persist",
		Short: "persist data from the experiment",
		Run: func(cmd *cobra.Command, args []string) {
			persist()
		},
	}

	return persistCommand
}
