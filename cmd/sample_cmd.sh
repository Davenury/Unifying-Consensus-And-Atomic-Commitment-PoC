#!/bin/bash

./ucac perform --monitoring-namespace=ddebowski \
--peers=$(python -c "print('$1,' * $2, end=''); print('$1')") --test-namespace=ddebowski --application-image=ghcr.io/davenury/ucac:8a722c69de4f5442656216810bf7b4fbf5323737 \
--performance-test-image=ghcr.io/davenury/tests:8a722c69de4f5442656216810bf7b4fbf5323737 --single-requests-number=0 --multiple-requests-number=1000 \
--test-duration=PT1M --max-peersets-in-change=2 --tests-strategy=delay_on_conflicts \
--ac-protocol=two_pc --performance-test-timeout-deadline=PT120M
