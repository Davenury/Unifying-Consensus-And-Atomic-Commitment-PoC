#!/bin/bash

./ucac perform --monitoring-namespace=ddebowski \
--peers=$(python -c "print('$1,' * $2, end=''); print('$1')") --test-namespace=ddebowski --application-image=ghcr.io/davenury/ucac:300f11428969012012e6f4b820bc89fccd5fde22 \
--performance-test-image=ghcr.io/davenury/tests:300f11428969012012e6f4b820bc89fccd5fde22 --single-requests-number=4000 --multiple-requests-number=5000 \
--test-duration=PT10M --max-peersets-in-change=2 --tests-strategy=delay_on_conflicts \
--ac-protocol=two_pc --enforce-ac --performance-test-timeout-deadline=PT120M
