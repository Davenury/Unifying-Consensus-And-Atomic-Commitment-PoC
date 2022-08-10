#!/bin/sh

# right now prometheus cannot connect to applications but can connect to grafana XD

docker run \
    -d --rm -p 9090:9090 \
    --net=host --name=prometheus \
    -v "$PWD"/prometheus.yml:/etc/prometheus/prometheus.yml \
    prom/prometheus

docker run -d --rm --name=grafana --net=host -p 3000:3000 grafana/grafana