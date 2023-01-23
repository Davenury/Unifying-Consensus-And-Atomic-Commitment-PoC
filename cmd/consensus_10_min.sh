#!/bin/bash

./ucac perform --monitoring-namespace=ddebowski \
--peers=$(python -c "print('$1,' * $2, end=''); print('$1')") --test-namespace=ddebowski --application-image=ghcr.io/davenury/ucac:1e211242f4e53136090de1c8acd280ba53e57ee0 \
--performance-test-image=ghcr.io/davenury/tests:1e211242f4e53136090de1c8acd280ba53e57ee0 --single-requests-number=3600 --multiple-requests-number=0 \
--test-duration=PT10M --max-peersets-in-change=1 --tests-strategy=delay_on_conflicts \
--ac-protocol=two_pc --performance-test-timeout-deadline=PT120M
