#!/bin/bash

./ucac perform --monitoring-namespace=ddebowski \
--peers=$(python3 -c "print('$1,' * $2, end=''); print('$1')") --test-namespace=ddebowski --application-image=ghcr.io/davenury/ucac:0cb27e1964f9b0fb517e7fe50ea958dbcce7390c \
--performance-test-image=ghcr.io/davenury/tests:0cb27e1964f9b0fb517e7fe50ea958dbcce7390c --constant-load=7 \
--fixed-peersets-in-change=2 --tests-sending-strategy=delay_on_conflicts \
--ac-protocol=gpac --enforce-ac --performance-test-timeout-deadline=PT120M --proxy-limit=0
