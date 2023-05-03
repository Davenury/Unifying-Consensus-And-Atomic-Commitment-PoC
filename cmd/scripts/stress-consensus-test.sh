#!/bin/bash

echo "Run protocol: $CONSENSUS"

./ucac perform --monitoring-namespace=rszuma \
--peers=$(python3 -c "print('$1,' * $2, end=''); print('$1')") --test-namespace=ddebowski --application-image=ghcr.io/davenury/ucac:900973c5feac7f0bfdfddea5074cad71faa99daf  \
--performance-test-image=ghcr.io/davenury/tests:900973c5feac7f0bfdfddea5074cad71faa99daf \
--fixed-peersets-in-change=1 --tests-sending-strategy=delay_on_conflicts \
--ac-protocol=two_pc --performance-test-timeout-deadline=PT120M --deploy-monitoring=true \
--consensus-protocol=${CONSENSUS:-raft} \
--load-generator-type=increasing --load-bound=1000 --load-increase-delay=PT20S --load-increase-step=5