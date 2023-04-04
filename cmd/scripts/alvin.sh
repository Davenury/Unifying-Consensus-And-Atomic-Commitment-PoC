#!/bin/bash
SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &> /dev/null && pwd)
"${SCRIPT_DIR}/../ucac" perform --monitoring-namespace=rszuma \
--peers=$(python3 -c "print('$1,' * $2, end=''); print('$1')") --test-namespace=ddebowski --application-image=ghcr.io/davenury/ucac:57f4cd39c759bee97b4229480989d2e53bace27a  \
--performance-test-image=ghcr.io/davenury/tests:57f4cd39c759bee97b4229480989d2e53bace27a --constant-load=5 --load-generator-type=constant \
--fixed-peersets-in-change=1 --tests-sending-strategy=delay_on_conflicts \
--consensus-protocol=alvin \
--ac-protocol=two_pc --performance-test-timeout-deadline=PT120M --deploy-monitoring=true
