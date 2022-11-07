package commands

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/goccy/kpoward"
	"github.com/spf13/cobra"
	"io/ioutil"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"net/http"
	"time"
)

const TEST_ITERATION_LIMIT = 60

var testNamespace string

func CreateTestCommand() *cobra.Command {

	var testCmd = &cobra.Command{
		Use:   "test",
		Short: "test if deployment works",
		Run: func(cmd *cobra.Command, args []string) {
			test()
		},
	}

	testCmd.Flags().StringVarP(&testNamespace, "namespace", "n", "", "Namespace with deployment")

	return testCmd
}

func test() {
	config, err := GetKubernetesConfig()
	if err != nil {
		panic(err)
	}

	client, err := GetClientset()
	if err != nil {
		panic(err)
	}

	pods, err := client.CoreV1().Pods(testNamespace).List(context.Background(), metav1.ListOptions{
		LabelSelector: "app.name=peer1-peerset1-app",
	})

	var podName string
	for _, pod := range pods.Items {
		podName = pod.Name
	}

	kpow := kpoward.New(config, podName, 8080)
	kpow.SetNamespace(testNamespace)

	if err := kpow.Run(context.Background(), func(ctx context.Context, localPort uint16) error {

		request := map[string]interface{}{
			"parentId": "27c74670adb75075fad058d5ceaf7b20c4e7786c83bae8a32f626f9782af34c9a33c2046ef60fd2a7878d378e29fec851806bbd9a67878f3a9f1cda4830763fd",
			"userName": "user1",
			"peers":    []int{},
			"@type":    "ADD_USER",
		}
		requestData, err := json.Marshal(request)
		if err != nil {
			return err
		}

		resp, err := http.Post(fmt.Sprintf("http://localhost:%d/gpac/create_change", int(localPort)), "application/json", bytes.NewBuffer(requestData))
		if err != nil {
			return err
		}

		body, err := ioutil.ReadAll(resp.Body)
		fmt.Printf("Response from post - Status Code: %d, Response body: %s\n", resp.StatusCode, string(body))
		resp.Body.Close()

		iteration := 0

		for iteration < TEST_ITERATION_LIMIT {

			time.Sleep(1 * time.Second)

			resp, err = http.Get(fmt.Sprintf("http://localhost:%d/consensus/change", int(localPort)))
			if err != nil {
				panic(err)
			}

			body, err = ioutil.ReadAll(resp.Body)
			if err != nil {
				panic(err)
			}

			fmt.Printf("Response from get - Status Code: %d, Response body: %s\n", resp.StatusCode, string(body))
			resp.Body.Close()

			if resp.StatusCode == 200 {
				return nil
			}
			if resp.StatusCode != 404 {
				return errors.New(fmt.Sprintf("Status code of change is: %d", resp.StatusCode))
			}

			iteration++
	
		}

		return errors.New("Change wasn't applied")
	}); err != nil {
		panic(err)
	}

}
