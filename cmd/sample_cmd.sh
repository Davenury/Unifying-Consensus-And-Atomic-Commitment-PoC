#!/bin/bash

./ucac perform --monitoring-namespace=ddebowski \
--peers=$(python -c "print('$1,' * $2, end=''); print('$1')") --test-namespace=ddebowski --application-image=ghcr.io/davenury/ucac:a2343e4d5fe4ec27cc122c50930526bfae11234c \
--performance-test-image=ghcr.io/davenury/tests:a2343e4d5fe4ec27cc122c50930526bfae11234c --single-requests-number=0 --multiple-requests-number=1000 \
--test-duration=PT1M --max-peersets-in-change=2 --tests-strategy=delay_on_conflicts \
--ac-protocol=two_pc --performance-test-timeout-deadline=PT120M
