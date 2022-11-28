package performance

import "github.com/spf13/cobra"

func CreatePerformanceCommand() *cobra.Command {

	var cmd = &cobra.Command{
		Use: "performance",
		Short: "Manage performance tests",
		Run: func(cmd *cobra.Command, args []string) {},
	}

	cmd.AddCommand(createPerformanceDeployCommand())
	cmd.AddCommand(createCleanupCommand())

	return cmd
}