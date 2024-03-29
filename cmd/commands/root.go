package commands

import (
	"fmt"
	"github.com/davenury/ucac/cmd/commands/persistence"
	"os"

	"github.com/davenury/ucac/cmd/commands/performance"
	"github.com/spf13/cobra"
)

var rootCommand = &cobra.Command{
	Use:   "ucac",
	Short: "Command for performing various tasks on UCAC project",
	Run:   func(cmd *cobra.Command, args []string) {},
}

func Execute() {
	if err := rootCommand.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	rootCommand.AddCommand(CreateDeployCommand())
	rootCommand.AddCommand(CreateCleanupCommand())
	rootCommand.AddCommand(CreateInitCommand())
	rootCommand.AddCommand(CreateTestCommand())
	rootCommand.AddCommand(performance.CreatePerformanceCommand())
	rootCommand.AddCommand(CreateWholeCommand())
	rootCommand.AddCommand(persistence.CreatePersistCommand())
}
