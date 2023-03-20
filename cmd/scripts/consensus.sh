#!/bin/bash

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &> /dev/null && pwd)
"${SCRIPT_DIR}/../ucac" perform --monitoring-namespace=ddebowski \
--peers=$(python3 -c "print('$1,' * $2, end=''); print('$1')") --test-namespace=kjarosz --application-image=ghcr.io/davenury/ucac:bc30be1cb0e06873f28195104ace6bd8b1a24232  \
--performance-test-image=ghcr.io/davenury/tests:bc30be1cb0e06873f28195104ace6bd8b1a24232 --constant-load=5 --load-generator-type=constant \
>>>>>>> d5df5f51936bdea3753658dde47ccc2f3399e6ae
--fixed-peersets-in-change=1 --tests-sending-strategy=delay_on_conflicts \
--ac-protocol=two_pc --performance-test-timeout-deadline=PT120M --deploy-monitoring=false
