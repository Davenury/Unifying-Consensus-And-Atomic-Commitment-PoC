#!/bin/bash

./ucac brr --monitoring-namespace=test --create-monitoring-namespace \
--peers=1,1 --create-test-namespace --test-namespace=test --application-image=ghcr.io/davenury/ucac:latest \
--performance-test-image=ghcr.io/davenury/tests:f577893a17972cc379198f081f01d4460c547486 --single-requests-number=1 --multiple-requests-number=0 \
--test-duration=PT2S --max-peersets-in-change=2 --tests-strategy=delay_on_conflicts --enforce-ac \
--ac-protocol=gpac --performance-test-timeout-deadline=PT5M