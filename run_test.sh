directory=$(dirname $0)
echo $directory

#protocols=("gpac" "two_pc")
protocols=("two_pc")

echo "Script directory: $directory"
cd "$directory/misc/grafana-scrapping" && npm i

test_type=1

initial_sleep="4m"
stress_sleep="1m"
finish_sleep="0m"

for peerset_size in {3,4,5,6,10,20}; do
  for protocol in $protocols; do
    echo "Run experiment type for protocol: $protocol for peerset_size: $peerset_size"

    echo "Constant load script"
    cd $directory/cmd && CONSENSUS=$protocol "./scripts/$protocol.sh" $peerset_size 1 &
    scriptPID=$!
    START_LEADER=$(date +%s%3N)
    sleep 1m
    grafana_pod=$(kubectl get pods -n=ddebowski | grep "grafana" | awk {'print $1'})
    echo "grafana pods: $grafana_pod"
    kubectl port-forward $grafana_pod 3000:3000 -n=ddebowski &
    portForwardPID=$!
    sleep $initial_sleep

    if [[ $test_type -eq 0 ]]; then
      test_name="stress-test"
      echo "$test_name - During processing changes"
      START_TIMESTAMP=$(date +%s%3N)
      sleep $stress_sleep
      END_TIMESTAMP=$(date +%s%3N)
      BASE_DOWNLOAD_PATH="$directory/misc/data-processing/$test_name-during-processing-changes" SCRAPING_TYPE="all" EXPERIMENT="${peerset_size}x2" PROTOCOL="$protocol" START_TIMESTAMP=$START_TIMESTAMP END_TIMESTAMP=$END_TIMESTAMP node "$directory/misc/grafana-scrapping/index.js"
      BASE_DOWNLOAD_PATH="$directory/misc/data-processing/$test_name-during-processing-changes" SCRAPING_TYPE="leader" EXPERIMENT="${peerset_size}x2" PROTOCOL="$protocol" START_TIMESTAMP=$START_LEADER END_TIMESTAMP=$END_TIMESTAMP node "$directory/misc/grafana-scrapping/index.js"
    elif [[ $test_type -eq 1 ]]; then
      test_name="ft-half-followers"
      echo "$test_name - During processing changes"
      START_TIMESTAMP=$(date +%s%3N)
      sleep $ft_sleep

      for i in $(seq 0 $ft_repeat); do
        echo "$test_name iteration $i"
        kubectl -n=ddebowski delete scenarioes.lsc.davenury.github.com gpac-chaos-delete-followers
        kubectl apply -f "$directory/cmd/yamls/delete_peers.yaml"
        sleep $ft_sleep
      done

      sleep $ft_sleep
      sleep 2m

      END_TIMESTAMP=$(date +%s%3N)

      BASE_DOWNLOAD_PATH="$directory/misc/data-processing/$test_name-after-deleting-article" SCRAPING_TYPE="all" EXPERIMENT="${peerset_size}x1" PROTOCOL=$protocol START_TIMESTAMP=$START_TIMESTAMP END_TIMESTAMP=$END_TIMESTAMP node "$directory/misc/grafana-scrapping/index.js"
      BASE_DOWNLOAD_PATH="$directory/misc/data-processing/$test_name-after-deleting-article" SCRAPING_TYPE="chaos" EXPERIMENT="${peerset_size}x1" PROTOCOL=$protocol START_TIMESTAMP=$START_TIMESTAMP END_TIMESTAMP=$END_TIMESTAMP node "$directory/misc/grafana-scrapping/index.js"
      BASE_DOWNLOAD_PATH="$directory/misc/data-processing/$test_name-after-deleting-article" SCRAPING_TYPE="leader" EXPERIMENT="${peerset_size}x1" PROTOCOL=$protocol START_TIMESTAMP=$START_LEADER END_TIMESTAMP=$END_TIMESTAMP node "$directory/misc/grafana-scrapping/index.js"
    fi

    sleep $finish_sleep

    echo "Cleanup state"
    cd $directory/cmd && "./ucac" cleanup -n=ddebowski
    cd $directory/cmd && "./ucac" cleanup -n=rszuma
    helm uninstall grafana -n=ddebowski &
    helm uninstall victoria loki -n=ddebowski &
    helm uninstall loki -n=ddebowski &
    helm uninstall tempo -n=ddebowski &
    kill $portForwardPID
    kill $scriptPID
    fuser -k 3000/tcp
    sleep 1m
  done
done
