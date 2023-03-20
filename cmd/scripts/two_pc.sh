#!/bin/bash
SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &> /dev/null && pwd)
"${SCRIPT_DIR}/./ucac" perform --monitoring-namespace=ddebowski \
--peers=$(python3 -c "print('$1,' * $2, end=''); print('$1')") --test-namespace=kjarosz --application-image=ghcr.io/davenury/ucac:bc30be1cb0e06873f28195104ace6bd8b1a24232 \
--performance-test-image=ghcr.io/davenury/tests:bc30be1cb0e06873f28195104ace6bd8b1a24232 --constant-load=5 --load-generator-type=constant \
--fixed-peersets-in-change=2 --tests-sending-strategy=delay_on_conflicts \
--ac-protocol=two_pc --enforce-ac --performance-test-timeout-deadline=PT120M --proxy-limit=0 --deploy-monitoring=false
