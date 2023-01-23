#!/bin/bash

./ucac perform --monitoring-namespace=ddebowski \
--peers=3,3 --test-namespace=ddebowski --application-image=ghcr.io/davenury/ucac:680821eb59c506100ed4ad82091a06cb10d585a5 \
--performance-test-image=ghcr.io/davenury/tests:680821eb59c506100ed4ad82091a06cb10d585a5 --single-requests-number=40 --multiple-requests-number=20 \
--test-duration=PT1M --max-peersets-in-change=2 --tests-strategy=delay_on_conflicts \
--ac-protocol=gpac --performance-test-timeout-deadline=PT120M
