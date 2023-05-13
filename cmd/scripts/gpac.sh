#!/bin/bash
SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &> /dev/null && pwd)
"${SCRIPT_DIR}/../ucac" perform --monitoring-namespace=ddebowski \
--peers=$(python3 -c "print('$1,' * $2, end=''); print('$1')") --test-namespace=ddebowski --application-image=ghcr.io/davenury/ucac:172ae5a02facba6319d1bfce3b3acc25f2cb4a4b \
--performance-test-image=ghcr.io/davenury/tests:39ae6870538d8906a4c3a77f494ebc9316b1cd8b --constant-load=5 --load-generator-type=constant \
--fixed-peersets-in-change=2 --tests-sending-strategy=random \
--ac-protocol=gpac --enforce-ac --performance-test-timeout-deadline=PT120M --proxy-limit=0 --enforce-consensus-leader=false
