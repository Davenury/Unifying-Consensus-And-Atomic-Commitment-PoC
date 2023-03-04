#!/bin/bash

./ucac perform --monitoring-namespace=ddebowski \
--peers=$(python3 -c "print('$1,' * $2, end=''); print('$1')") --test-namespace=ddebowski --application-image=ghcr.io/davenury/ucac:d6764c45a66c62aad0458fe1a43f20a81a72e275 \
--performance-test-image=ghcr.io/davenury/tests:d6764c45a66c62aad0458fe1a43f20a81a72e275 --constant-load=5 \
--fixed-peersets-in-change=2 --tests-sending-strategy=delay_on_conflicts \
--ac-protocol=gpac --enforce-ac --performance-test-timeout-deadline=PT120M --proxy-limit=0
