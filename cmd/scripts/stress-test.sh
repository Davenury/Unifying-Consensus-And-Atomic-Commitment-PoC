#!/bin/bash
SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &> /dev/null && pwd)
"${SCRIPT_DIR}/../ucac" perform --monitoring-namespace=ddebowski \
--peers=$(python3 -c "print('$1,' * $2, end=''); print('$1')") --test-namespace=ddebowski --application-image=ghcr.io/davenury/ucac:085a56bc8b0889d098ec61da001022206fd1cade \
--performance-test-image=ghcr.io/davenury/tests:085a56bc8b0889d098ec61da001022206fd1cade \
--fixed-peersets-in-change=2 --tests-sending-strategy=delay_on_conflicts \
--ac-protocol=gpac --enforce-ac --performance-test-timeout-deadline=PT120M --proxy-limit=0 \
--load-generator-type=increasing --load-bound=100 --load-increase-delay=PT60S --load-increase-step=1
