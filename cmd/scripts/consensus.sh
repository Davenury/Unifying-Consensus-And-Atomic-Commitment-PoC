#!/bin/bash

echo "Run protocol: $CONSENSUS"

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &> /dev/null && pwd)
"${SCRIPT_DIR}/../ucac" perform --monitoring-namespace=rszuma \
--peers=$(python3 -c "print('$1,' * $2, end=''); print('$1')") --test-namespace=ddebowski --application-image=ghcr.io/davenury/ucac:00a6514f93cf9ebcc89efd9bf61a7847c5a04507  \
--performance-test-image=ghcr.io/davenury/tests:00a6514f93cf9ebcc89efd9bf61a7847c5a04507 --constant-load=5 --load-generator-type=constant \
--fixed-peersets-in-change=1 --tests-sending-strategy=delay_on_conflicts \
--consensus-protocol=${CONSENSUS:-raft} \
--ac-protocol=two_pc --performance-test-timeout-deadline=PT120M --deploy-monitoring=true
