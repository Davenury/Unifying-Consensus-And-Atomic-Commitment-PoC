directory=$(dirname $0)
echo $directory
"$directory/../ucac" cleanup -n=rszuma
"$directory/../ucac" cleanup -n=ddebowski
helm uninstall grafana -n=rszuma &
helm uninstall victoria loki -n=rszuma &
helm uninstall loki -n=rszuma &
helm uninstall tempo -n=rszuma &