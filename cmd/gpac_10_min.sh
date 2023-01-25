#!/bin/bash

./ucac perform --monitoring-namespace=ddebowski \
--peers=$(python -c "print('$1,' * $2, end=''); print('$1')") --test-namespace=ddebowski --application-image=ghcr.io/davenury/ucac:936776953955857e1562a4dc2809f6b454cd242c \
--performance-test-image=ghcr.io/davenury/tests:936776953955857e1562a4dc2809f6b454cd242c --single-requests-number=0 --multiple-requests-number=1 \
--test-duration=PT10M --max-peersets-in-change=2 --tests-strategy=delay_on_conflicts \
--ac-protocol=gpac --enforce-ac --performance-test-timeout-deadline=PT120M
