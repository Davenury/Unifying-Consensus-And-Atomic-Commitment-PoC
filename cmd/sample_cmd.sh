#!/bin/bash

./ucac perform --monitoring-namespace=ddebowski \
--peers=3,7,5,3 --test-namespace=ddebowski --application-image=ghcr.io/davenury/ucac:latest \
--performance-test-image=ghcr.io/davenury/tests:latest --single-requests-number=30 --multiple-requests-number=50 \
--test-duration=PT3M --max-peersets-in-change=2 --tests-strategy=delay_on_conflicts \
--ac-protocol=gpac --performance-test-timeout-deadline=PT10M
