directory=$(dirname $0)
echo $directory

protocols=("alvin" "paxos" "raft")
scripts=("consensus" "stress-consensus-test")

peerset_size_start=3
peerset_size_end=5

echo "Script directory: $directory"
cd "$directory/misc/grafana-scrapping" && npm i

for peerset_size in $(seq $peerset_size_start $peerset_size_end)
do
  for protocol in $protocols
  do
    for script in $scripts
    do
      echo "Run experiment for protocol: $protocol for peerset_size: $peerset_size"
      cd $directory/cmd && CONSENSUS=$protocol "./scripts/$script.sh" $peerset_size 0 &
      scriptPID=$!
      sleep 1m
      grafana_pod=$(kubectl get pods -n=rszuma | grep "grafana" | awk {'print $1'})
      echo "grafana pods: $grafana_pod"
      kubectl port-forward $grafana_pod 3000:3000 -n=rszuma &
      portForwardPID=$!
      sleep 3m
      START_TIMESTAMP=$(date +%s%3N)
      echo "Grafana start: $START_TIMESTAMP"
      sleep 3m
      END_TIMESTAMP=$(date +%s%3N)
      echo "Grafana end: $END_TIMESTAMP"
      BASE_DOWNLOAD_PATH="$directory/misc/data-processing/$script" EXPERIMENT="${peerset_size}x1" PROTOCOL=$protocol START_TIMESTAMP=$START_TIMESTAMP END_TIMESTAMP=$END_TIMESTAMP node "$directory/misc/grafana-scrapping/index.js"

      echo "Cleanup state"
      cd $directory/cmd && "./ucac" cleanup -n=rszuma
      cd $directory/cmd && "./ucac" cleanup -n=ddebowski
      helm uninstall grafana -n=rszuma &
      helm uninstall victoria loki -n=rszuma &
      helm uninstall loki -n=rszuma &
      kill $portForwardPID
      kill $scriptPID
      fuser -k 3000/tcp
      sleep 1m
    done
  done
done





