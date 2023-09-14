directory=$(dirname $0)
echo $directory

#protocols=("alvin" "paxos" "raft")
#protocols=("alvin")
protocol="raft"
#protocol="alvin"
#protocol="paxos"
#scripts=("consensus" "stress-consensus-test")
#scripts=("stress-consensus-test" )
#scripts=("consensus")
script="consensus"

echo "Script directory: $directory"

cd "$directory/cmd" && "./scripts/clean_chaos.sh"
cd "$directory/cmd" && "./scripts/install_chaos.sh"
cd "$directory/cmd" && "./scripts/clean_chaos.sh"
cd "$directory/cmd" && "./scripts/install_chaos.sh"
kubectl -n=ddebowski delete scenarioes.lsc.davenury.github.com gpac-chaos-delete-followers

kubectl ns rszuma

cd "$directory/misc/grafana-scrapping" && npm i

peerset_size=3

echo "Run experiment for protocol: $protocol for peerset_size: $peerset_size"
cd $directory/cmd && CONSENSUS=$protocol "./scripts/$script.sh" $peerset_size 0 &
START_TIMESTAMP=$(date +%s%3N)
scriptPID=$!
sleep 1m
grafana_pod=$(kubectl get pods -n=rszuma | grep "grafana" | awk {'print $1'})
echo "grafana pods: $grafana_pod"
kubectl port-forward $grafana_pod 3000:3000 -n=rszuma &
portForwardPID=$!
echo "During processing changes"
sleep 5m

echo "Delete peer"
kubectl apply -f "$directory/cmd/yamls/delete_peer.yaml"


sleep 10m

END_TIMESTAMP=$(date +%s%3N)
sleep 1m
#BASE_DOWNLOAD_PATH="$directory/misc/data-processing/$script-ft-test" IS_CHANGE_PROCESSED=true EXPERIMENT="${peerset_size}x1" PROTOCOL=$protocol START_TIMESTAMP=$START_TIMESTAMP END_TIMESTAMP=$END_TIMESTAMP node "$directory/misc/grafana-scrapping/index.js"


#echo "Cleanup state"
#cd $directory/cmd && "./ucac" cleanup -n=rszuma
#cd $directory/cmd && "./ucac" cleanup -n=ddebowski
#helm uninstall grafana -n=rszuma &
#helm uninstall victoria loki -n=rszuma &
#helm uninstall loki -n=rszuma &
#helm uninstall tempo -n=rszuma &
#kill $portForwardPID
#kill $scriptPID
#fuser -k 3000/tcp
#sleep 1m