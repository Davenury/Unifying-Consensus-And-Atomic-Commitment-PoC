helm uninstall chaos
helm repo remove chaos

kubectl -n=chaos-operator delete deployments.apps chaos-operator
kubectl -n=chaos-operator delete services chaos-operator-service
kubectl delete namespace chaos-operator