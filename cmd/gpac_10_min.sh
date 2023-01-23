#!/bin/bash

./ucac perform --monitoring-namespace=ddebowski \
--peers=$(python -c "print('$1,' * $2, end=''); print('$1')") --test-namespace=ddebowski --application-image=ghcr.io/davenury/ucac:3ee83256e95b31638507874823d9eb77b4a7bce4 \
--performance-test-image=ghcr.io/davenury/tests:3ee83256e95b31638507874823d9eb77b4a7bce4 --single-requests-number=0 --multiple-requests-number=3600 \
--test-duration=PT10M --max-peersets-in-change=2 --tests-strategy=delay_on_conflicts \
--ac-protocol=gpac --enforce-ac --performance-test-timeout-deadline=PT120M
