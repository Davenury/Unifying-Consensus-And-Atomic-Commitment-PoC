#!/bin/bash

./ucac perform --monitoring-namespace=ddebowski \
--peers=$(python -c "print('$1,' * $2, end=''); print('$1')") --test-namespace=ddebowski --application-image=ghcr.io/davenury/ucac:c9014fe6e9c8293a33d0adadf19ac48dc462d8a3 \
--performance-test-image=ghcr.io/davenury/tests:c9014fe6e9c8293a33d0adadf19ac48dc462d8a3 --single-requests-number=10000 --multiple-requests-number=20000 \
--test-duration=PT10M --max-peersets-in-change=2 --tests-strategy=delay_on_conflicts \
--ac-protocol=gpac --performance-test-timeout-deadline=PT120M
