#!/bin/bash

echo "Run protocol: $CONSENSUS"

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &> /dev/null && pwd)
"${SCRIPT_DIR}/../ucac" perform --monitoring-namespace=rszuma \
--peers=$(python3 -c "print('$1,' * $2, end=''); print('$1')") --test-namespace=ddebowski --application-image=ghcr.io/davenury/ucac:3b39fa5511dd67f08fa2c7eda4a1c1fdd01a61f8  \
--performance-test-image=ghcr.io/davenury/tests:3b39fa5511dd67f08fa2c7eda4a1c1fdd01a61f8 --constant-load=5 --load-generator-type=constant \
--fixed-peersets-in-change=1 --tests-sending-strategy=delay_on_conflicts \
--consensus-protocol=${CONSENSUS:-raft} \
--ac-protocol=two_pc --performance-test-timeout-deadline=PT120M --deploy-monitoring=true
