#!/bin/bash

./ucac perform --monitoring-namespace=ddebowski \
--peers=3,3 --test-namespace=ddebowski --application-image=ghcr.io/davenury/ucac:1e211242f4e53136090de1c8acd280ba53e57ee0 \
--performance-test-image=ghcr.io/davenury/tests:1e211242f4e53136090de1c8acd280ba53e57ee0 --single-requests-number=40 --multiple-requests-number=20 \
--test-duration=PT1M --max-peersets-in-change=2 --tests-strategy=delay_on_conflicts \
--ac-protocol=gpac --performance-test-timeout-deadline=PT120M
