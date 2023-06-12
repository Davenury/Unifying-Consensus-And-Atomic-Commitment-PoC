for peerId in $(seq 0 4); do
  peer=$(kubectl get pods -n=ddebowski | grep "peer$peerId" | awk {'print $1'})
  kubectl -n=ddebowski logs $peer > "peer$peerId.logs"
done

peer=$(kubectl get pods -n=ddebowski | grep "performance" | awk {'print $1'})
kubectl -n=ddebowski logs $peer > "performance.logs"
kubectl -n=ddebowski describe pods > "describe_pods.logs"