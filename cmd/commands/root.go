package commands

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var rootCommand = &cobra.Command{
	Use: "ucac",
	Short: "Command for performing various tasks on UCAC project",
	Run: func(cmd *cobra.Command, args []string) {},
}

func Execute() {
	if err := rootCommand.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	rootCommand.AddCommand(CreateTestCommand())
	rootCommand.AddCommand(CreateDeployCommand())
}