#!/bin/bash

./ucac perform --monitoring-namespace=ddebowski \
--peers=$(python -c "print('$1,' * $2, end=''); print('$1')") --test-namespace=ddebowski --application-image=ghcr.io/davenury/ucac:680821eb59c506100ed4ad82091a06cb10d585a5 \
--performance-test-image=ghcr.io/davenury/tests:680821eb59c506100ed4ad82091a06cb10d585a5 --single-requests-number=3600 --multiple-requests-number=0 \
--test-duration=PT10M --max-peersets-in-change=1 --tests-strategy=delay_on_conflicts \
--ac-protocol=two_pc --performance-test-timeout-deadline=PT120M
