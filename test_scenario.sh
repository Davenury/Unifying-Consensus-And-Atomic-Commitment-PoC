directory=$(dirname $0)
echo $directory

protocols=("alvin" "paxos" "raft")
#protocols=("alvin" )
#protocols=("paxos" )

peerset_size_start=5
peerset_size_end=5

echo "Script directory: $directory"
cd "$directory/misc/grafana-scrapping" && npm i

# Test scenarios:
# 0. Stress tests
# 1. Constant load, resource usage during processing changes and after
# 2. FT tests after killing follower repeated n times
# 3. FT tests after killing leader repeated n times
# 4. FT tests after killing half of followers

ft_repeat=0

start_test=3
end_test=4

initial_sleep="3m"
resource_sleep="3m"
ft_sleep="3m"
finish_sleep="3m"

for peerset_size in $(seq $peerset_size_start $peerset_size_end); do
  for protocol in $protocols; do
    for test_type in $(seq $start_test $end_test); do
      echo "Run experiment type $test_type for protocol: $protocol for peerset_size: $peerset_size"
      if [[ $test_type -eq 0 ]]; then
        echo "Increasing load script"
        cd $directory/cmd && CONSENSUS=$protocol "./scripts/stress-consensus-test.sh" $peerset_size 0 &
      else
        echo "Constant load script"
        cd $directory/cmd && CONSENSUS=$protocol "./scripts/consensus.sh" $peerset_size 0 &
      fi
      scriptPID=$!
      START_LEADER=$(date +%s%3N)
      sleep 1m
      grafana_pod=$(kubectl get pods -n=rszuma | grep "grafana" | awk {'print $1'})
      echo "grafana pods: $grafana_pod"
      kubectl port-forward $grafana_pod 3000:3000 -n=rszuma &
      portForwardPID=$!
      sleep $initial_sleep

      if [[ $test_type -eq 0 ]]; then
        test_name="stress-test"
        echo "$test_name - During processing changes"
        START_TIMESTAMP=$(date +%s%3N)
        sleep 2m
        END_TIMESTAMP=$(date +%s%3N)
        BASE_DOWNLOAD_PATH="$directory/misc/data-processing/$test_name-during-processing-changes" SCRAPING_TYPE="all" EXPERIMENT="${peerset_size}x1" PROTOCOL=$protocol START_TIMESTAMP=$START_TIMESTAMP END_TIMESTAMP=$END_TIMESTAMP node "$directory/misc/grafana-scrapping/index.js"
        BASE_DOWNLOAD_PATH="$directory/misc/data-processing/$test_name-during-processing-changes" SCRAPING_TYPE="leader" EXPERIMENT="${peerset_size}x1" PROTOCOL=$protocol START_TIMESTAMP=$START_LEADER END_TIMESTAMP=$END_TIMESTAMP node "$directory/misc/grafana-scrapping/index.js"
      elif [[ $test_type -eq 1 ]]; then
        test_name="resource-usage"
        echo "$test_name - During processing changes"
        START_TIMESTAMP=$(date +%s%3N)
        sleep $resource_sleep
        END_TIMESTAMP=$(date +%s%3N)
        sleep 1m
        BASE_DOWNLOAD_PATH="$directory/misc/data-processing/$test_name-during-processing-changes" SCRAPING_TYPE="all" EXPERIMENT="${peerset_size}x1" PROTOCOL=$protocol START_TIMESTAMP=$START_TIMESTAMP END_TIMESTAMP=$END_TIMESTAMP node "$directory/misc/grafana-scrapping/index.js"
        BASE_DOWNLOAD_PATH="$directory/misc/data-processing/$test_name-during-processing-changes" SCRAPING_TYPE="leader" EXPERIMENT="${peerset_size}x1" PROTOCOL=$protocol START_TIMESTAMP=START_LEADER END_TIMESTAMP=$END_TIMESTAMP node "$directory/misc/grafana-scrapping/index.js"

        echo "$test_name - After changes"
        kubectl delete -n=ddebowski jobs.batch performance-test
        sleep 1m
        START_TIMESTAMP=$(date +%s%3N)
        sleep $resource_sleep
        END_TIMESTAMP=$(date +%s%3N)
        sleep 1m
        BASE_DOWNLOAD_PATH="$directory/misc/data-processing/$test_name-after-processing-changes" SCRAPING_TYPE="without-changes" EXPERIMENT="${peerset_size}x1" PROTOCOL=$protocol START_TIMESTAMP=$START_TIMESTAMP END_TIMESTAMP=$END_TIMESTAMP node "$directory/misc/grafana-scrapping/index.js"
        BASE_DOWNLOAD_PATH="$directory/misc/data-processing/$test_name-after-processing-changes" SCRAPING_TYPE="leader" EXPERIMENT="${peerset_size}x1" PROTOCOL=$protocol START_TIMESTAMP=$START_LEADER END_TIMESTAMP=$END_TIMESTAMP node "$directory/misc/grafana-scrapping/index.js"
      elif [[ $test_type -eq 2 ]]; then
        test_name="ft-follower"
        echo "$test_name - During processing changes"
        START_TIMESTAMP=$(date +%s%3N)
        sleep $ft_sleep

        for i in $(seq 0 $ft_repeat); do
          echo "$test_name iteration $i"
          kubectl -n=ddebowski delete scenarioes.lsc.davenury.github.com gpac-chaos-delete-one-follower
          kubectl apply -f "$directory/cmd/yamls/delete_peer.yaml"
          sleep $ft_sleep
        done

        sleep $ft_sleep

        END_TIMESTAMP=$(date +%s%3N)

        BASE_DOWNLOAD_PATH="$directory/misc/data-processing/$test_name-after-deleting-follower" SCRAPING_TYPE="synchronization" EXPERIMENT="${peerset_size}x1" PROTOCOL=$protocol START_TIMESTAMP=$START_TIMESTAMP END_TIMESTAMP=$END_TIMESTAMP node "$directory/misc/grafana-scrapping/index.js"
        BASE_DOWNLOAD_PATH="$directory/misc/data-processing/$test_name-after-deleting-follower" SCRAPING_TYPE="leader" EXPERIMENT="${peerset_size}x1" PROTOCOL=$protocol START_TIMESTAMP=$START_LEADER END_TIMESTAMP=$END_TIMESTAMP node "$directory/misc/grafana-scrapping/index.js"
        BASE_DOWNLOAD_PATH="$directory/misc/data-processing/$test_name-after-deleting-follower" SCRAPING_TYPE="chaos" EXPERIMENT="${peerset_size}x1" PROTOCOL=$protocol START_TIMESTAMP=$START_LEADER END_TIMESTAMP=$END_TIMESTAMP node "$directory/misc/grafana-scrapping/index.js"
      elif [[ $test_type -eq 3 ]]; then
        test_name="ft-leader"
        echo "$test_name - During processing changes"
        START_TIMESTAMP=$(date +%s%3N)
        sleep $ft_sleep

        for i in $(seq 0 $ft_repeat); do
          echo "$test_name iteration $i"
          kubectl -n=ddebowski delete scenarioes.lsc.davenury.github.com gpac-chaos-delete-leader
          kubectl apply -f "$directory/cmd/yamls/delete_leader.yaml"
          sleep $ft_sleep
        done

        sleep $ft_sleep

        END_TIMESTAMP=$(date +%s%3N)

        BASE_DOWNLOAD_PATH="$directory/misc/data-processing/$test_name-after-deleting-leaders" SCRAPING_TYPE="synchronization" EXPERIMENT="${peerset_size}x1" PROTOCOL=$protocol START_TIMESTAMP=$START_TIMESTAMP END_TIMESTAMP=$END_TIMESTAMP node "$directory/misc/grafana-scrapping/index.js"
        BASE_DOWNLOAD_PATH="$directory/misc/data-processing/$test_name-after-deleting-leaders" SCRAPING_TYPE="chaos" EXPERIMENT="${peerset_size}x1" PROTOCOL=$protocol START_TIMESTAMP=$START_TIMESTAMP END_TIMESTAMP=$END_TIMESTAMP node "$directory/misc/grafana-scrapping/index.js"
        BASE_DOWNLOAD_PATH="$directory/misc/data-processing/$test_name-after-deleting-leaders" SCRAPING_TYPE="leader" EXPERIMENT="${peerset_size}x1" PROTOCOL=$protocol START_TIMESTAMP=$START_LEADER END_TIMESTAMP=$END_TIMESTAMP node "$directory/misc/grafana-scrapping/index.js"
      elif [[ $test_type -eq 4 ]]; then
        test_name="ft-half-followers"
        echo "$test_name - During processing changes"
        START_TIMESTAMP=$(date +%s%3N)
        sleep $ft_sleep

        for i in $(seq 0 $ft_repeat); do
          echo "$test_name iteration $i"
          kubectl -n=ddebowski delete scenarioes.lsc.davenury.github.com gpac-chaos-delete-two-followers
          kubectl apply -f "$directory/cmd/yamls/delete_half_peers.yaml"
          sleep $ft_sleep
        done

        sleep $ft_sleep

        END_TIMESTAMP=$(date +%s%3N)

        BASE_DOWNLOAD_PATH="$directory/misc/data-processing/$test_name-after-deleting-two-peers" SCRAPING_TYPE="all" EXPERIMENT="${peerset_size}x1" PROTOCOL=$protocol START_TIMESTAMP=$START_TIMESTAMP END_TIMESTAMP=$END_TIMESTAMP node "$directory/misc/grafana-scrapping/index.js"
        BASE_DOWNLOAD_PATH="$directory/misc/data-processing/$test_name-after-deleting-two-peers" SCRAPING_TYPE="chaos" EXPERIMENT="${peerset_size}x1" PROTOCOL=$protocol START_TIMESTAMP=$START_TIMESTAMP END_TIMESTAMP=$END_TIMESTAMP node "$directory/misc/grafana-scrapping/index.js"
        BASE_DOWNLOAD_PATH="$directory/misc/data-processing/$test_name-after-deleting-two-peers" SCRAPING_TYPE="leader" EXPERIMENT="${peerset_size}x1" PROTOCOL=$protocol START_TIMESTAMP=$START_LEADER END_TIMESTAMP=$END_TIMESTAMP node "$directory/misc/grafana-scrapping/index.js"
      fi

      sleep $finish_sleep

      echo "Cleanup state"
      cd $directory/cmd && "./ucac" cleanup -n=rszuma
      cd $directory/cmd && "./ucac" cleanup -n=ddebowski
      helm uninstall grafana -n=rszuma &
      helm uninstall victoria loki -n=rszuma &
      helm uninstall loki -n=rszuma &
      helm uninstall tempo -n=rszuma &
      kill $portForwardPID
      kill $scriptPID
      fuser -k 3000/tcp
      sleep 1m
    done
  done
done
