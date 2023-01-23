#!/bin/bash

./ucac perform --monitoring-namespace=ddebowski \
--peers=$(python -c "print('$1,' * $2, end=''); print('$1')") --test-namespace=ddebowski --application-image=ghcr.io/davenury/ucac:13bcfb0aeeb0d586376b259bc48b4ecdbabdb6b8 \
--performance-test-image=ghcr.io/davenury/tests:13bcfb0aeeb0d586376b259bc48b4ecdbabdb6b8 --single-requests-number=0 --multiple-requests-number=1000 \
--test-duration=PT1M --max-peersets-in-change=2 --tests-strategy=delay_on_conflicts \
--ac-protocol=two_pc --performance-test-timeout-deadline=PT120M
