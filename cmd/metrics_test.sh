#!/bin/bash

./ucac perform --monitoring-namespace=ddebowski \
--peers=3,3 --test-namespace=ddebowski --application-image=ghcr.io/davenury/ucac:13bcfb0aeeb0d586376b259bc48b4ecdbabdb6b8 \
--performance-test-image=ghcr.io/davenury/tests:13bcfb0aeeb0d586376b259bc48b4ecdbabdb6b8 --single-requests-number=40 --multiple-requests-number=20 \
--test-duration=PT1M --max-peersets-in-change=2 --tests-strategy=delay_on_conflicts \
--ac-protocol=gpac --performance-test-timeout-deadline=PT120M
