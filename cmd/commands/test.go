package commands

import (
	"fmt"

	"github.com/spf13/cobra"
)

var testFlag string

func CreateTestCommand() *cobra.Command {

	var testCmd = &cobra.Command{
		Use: "echo",
		Short: "echo flags",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println(testFlag)
		},
	}

	testCmd.Flags().StringVarP(&testFlag, "test", "t", "", "Test flag")

	return testCmd
}