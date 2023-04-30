#!/bin/bash

echo "Run protocol: $CONSENSUS"

./ucac perform --monitoring-namespace=rszuma \
--peers=$(python3 -c "print('$1,' * $2, end=''); print('$1')") --test-namespace=ddebowski --application-image=ghcr.io/davenury/ucac:3497437dd3a673030c18d1b30a13e43857e3a88d  \
--performance-test-image=ghcr.io/davenury/tests:3497437dd3a673030c18d1b30a13e43857e3a88d \
--fixed-peersets-in-change=1 --tests-sending-strategy=delay_on_conflicts \
--ac-protocol=two_pc --performance-test-timeout-deadline=PT120M --deploy-monitoring=true \
--consensus-protocol=${CONSENSUS:-raft} \
--load-generator-type=increasing --load-bound=1000 --load-increase-delay=PT20S --load-increase-step=5