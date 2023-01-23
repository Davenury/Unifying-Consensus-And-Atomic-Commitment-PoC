#!/bin/bash

./ucac perform --monitoring-namespace=ddebowski \
--peers=$(python -c "print('$1,' * $2, end=''); print('$1')") --test-namespace=ddebowski --application-image=ghcr.io/davenury/ucac:285a28dcd8be1fa0a5168265842ada781e6f29c2 \
--performance-test-image=ghcr.io/davenury/tests:285a28dcd8be1fa0a5168265842ada781e6f29c2 --single-requests-number=0 --multiple-requests-number=3600 \
--test-duration=PT10M --max-peersets-in-change=2 --tests-strategy=delay_on_conflicts \
--ac-protocol=gpac --enforce-ac --performance-test-timeout-deadline=PT120M
