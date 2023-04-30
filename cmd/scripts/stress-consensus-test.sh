#!/bin/bash

echo "Run protocol: $CONSENSUS"

./ucac perform --monitoring-namespace=rszuma \
--peers=$(python3 -c "print('$1,' * $2, end=''); print('$1')") --test-namespace=ddebowski --application-image=ghcr.io/davenury/ucac:af66ef035a7e43eec35a34c39d10adca0c1bff6a  \
--performance-test-image=ghcr.io/davenury/tests:af66ef035a7e43eec35a34c39d10adca0c1bff6a \
--fixed-peersets-in-change=1 --tests-sending-strategy=delay_on_conflicts \
--ac-protocol=two_pc --performance-test-timeout-deadline=PT120M --deploy-monitoring=true \
--consensus-protocol=${CONSENSUS:-raft} \
--load-generator-type=increasing --load-bound=1000 --load-increase-delay=PT20S --load-increase-step=5