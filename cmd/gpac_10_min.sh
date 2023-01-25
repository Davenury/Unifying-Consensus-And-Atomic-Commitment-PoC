#!/bin/bash

./ucac perform --monitoring-namespace=ddebowski \
--peers=$(python -c "print('$1,' * $2, end=''); print('$1')") --test-namespace=ddebowski --application-image=ghcr.io/davenury/ucac:36be4a2a6608eab4a2e134366d7a294c31bf6405 \
--performance-test-image=ghcr.io/davenury/tests:36be4a2a6608eab4a2e134366d7a294c31bf6405 --constant-load=3 \
--fixed-peersets-in-change=2 --tests-strategy=delay_on_conflicts \
--ac-protocol=gpac --enforce-ac --performance-test-timeout-deadline=PT120M
