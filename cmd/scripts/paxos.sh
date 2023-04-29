#!/bin/bash
SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &> /dev/null && pwd)
"${SCRIPT_DIR}/../ucac" perform --monitoring-namespace=rszuma \
--peers=$(python3 -c "print('$1,' * $2, end=''); print('$1')") --test-namespace=ddebowski --application-image=ghcr.io/davenury/ucac:678b4658d76caacd540f6169dab4809090729e24  \
--performance-test-image=ghcr.io/davenury/tests:678b4658d76caacd540f6169dab4809090729e24 --constant-load=5 --load-generator-type=constant \
--fixed-peersets-in-change=1 --tests-sending-strategy=delay_on_conflicts \
--consensus-protocol=paxos \
--ac-protocol=two_pc --performance-test-timeout-deadline=PT120M --deploy-monitoring=true
