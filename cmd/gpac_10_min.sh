#!/bin/bash

./ucac perform --monitoring-namespace=ddebowski \
--peers=$(python -c "print('$1,' * $2, end=''); print('$1')") --test-namespace=ddebowski --application-image=ghcr.io/davenury/ucac:4c4369b1a44cbedf179fe7d2d2bcdc7ecadc122e \
--performance-test-image=ghcr.io/davenury/tests:4c4369b1a44cbedf179fe7d2d2bcdc7ecadc122e --constant-load=6 \
--fixed-peersets-in-change=2 --tests-strategy=delay_on_conflicts \
--ac-protocol=gpac --enforce-ac --performance-test-timeout-deadline=PT120M
