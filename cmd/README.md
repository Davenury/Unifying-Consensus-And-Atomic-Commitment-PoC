# Command

### Building command
Make sure you have installed:
* kubectl
* minikube
* helm
* golang
All those can be installed with `install.sh` script in `k8s-stuff` directory.
If you do, type
```
go get
go build -o ucac
```
`go get` is basically `npm install` for golang. `go build` will build binary file to use as a command. If you want you
can of course add it to path / move it somewhere but it doesn't really matter.

## Using command
There are a couple of available commands for you to use. Before using them, make sure you have minikube running (you 
can check by `minikube status` command). Command also has built-in help but if you need more, here's how to use it

### cleanup
Command for deleting all pods / deployments / services / configmaps from desired namespace. Deleting looks at 
k8s labels to determine if resource should be deleted (label `project=ucac`), so we'll delete only resources we've created.
Cleanup command won't delete prometheus and grafana though.

Sample usage:
```bash
./ucac cleanup -n tryout
```
where `tryout` is the namespace where everything was deployed.

### deploy
Deploy command will create all necessary k8s resources for system to work.
Flags:
* -n / --namespace - namespace to deploy system to
* --create-namespace - determines if namespace should be created
* --peers - requires sequence of ints separated by commas. Determines number of peers in peersets. E.g. --peers=2,1,3 means that first peerset will have 2 peers, second peerset - 1 and third peerset will have 3 peers

Pods deployed by this command automatically are scrapped by prometheus.

### init
Init command deploys prometheus and grafana by helm. Requires `--namespace` flag. You can see helm releases using
```bash
helm list -n <namespace>
```
As deploy command, init command comes with `--create-namespace` flag.

Before that make sure, that you have `victoria metrics` and `grafana` charts repositories:
```bash
helm repo add vm https://victoriametrics.github.io/helm-charts/
helm repo add stable https://charts.helm.sh/stable
helm repo add grafana https://grafana.github.io/helm-charts
helm repo update
```

## Architecture
What command actually deploys?
Each peer is a pod, managed by a deployment (one deployment for one pod). Each pod has service, that forwards requests.
Using services gives us simplicity of creating in-cluster urls even if pod restarts for some reason. There's one service for one pod.
Command also deploys configmap, which supplies pod with env variables. One configmap for one pod.

# Instruction on how to performance tests
1. Make sure you have helm charts installed:
```bash
helm repo add vm https://victoriametrics.github.io/helm-charts/
helm repo add stable https://charts.helm.sh/stable
helm repo add grafana https://grafana.github.io/helm-charts
helm repo update
```
2. Make sure the script you want to execute from `scripts` directory is executable.
3. Execute script from `cmd` directory, e.g `./scripts/gpac.sh 3 0`, where the first number is number of peers in peersets, the second number is (number of peersets-1). Example will produce two peersets with 3 peers in each.
4. To see the metrics on grafana, do port-forward. Grafana does not require login, the dashboard is loaded. Loki (for logs) is accessible from explore page.
5. Don't forget to cleanup after yourself. Pods - `ucac cleanup -n <namespace>`; monitoring - `helm delele grafana victoria loki`.
