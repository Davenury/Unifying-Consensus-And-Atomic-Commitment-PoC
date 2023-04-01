#!/bin/bash
SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &> /dev/null && pwd)
"${SCRIPT_DIR}/../ucac" perform --monitoring-namespace=ddebowski \
--peers=$(python3 -c "print('$1,' * $2, end=''); print('$1')") --test-namespace=rszuma --application-image=ghcr.io/davenury/ucac:2fde267f12e1bfbdc05ecd163280dca8f62d8ad0  \
--performance-test-image=ghcr.io/davenury/tests:2fde267f12e1bfbdc05ecd163280dca8f62d8ad0 --constant-load=5 --load-generator-type=constant \
--fixed-peersets-in-change=1 --tests-sending-strategy=delay_on_conflicts \
--consensus-protocol=alvin \
--ac-protocol=two_pc --performance-test-timeout-deadline=PT120M --deploy-monitoring=false
