#!/bin/bash

./ucac perform --monitoring-namespace=ddebowski \
--peers=$(python -c "print('$1,' * $2, end=''); print('$1')") --test-namespace=ddebowski --application-image=ghcr.io/davenury/ucac:bd89efd4bbbe7c6d664df591b16ae5f660b09ba9 \
--performance-test-image=ghcr.io/davenury/tests:bd89efd4bbbe7c6d664df591b16ae5f660b09ba9 --constant-load=7 \
--max-peersets-in-change=2 --tests-strategy=delay_on_conflicts \
--ac-protocol=gpac --enforce-ac --performance-test-timeout-deadline=PT120M
