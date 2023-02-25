#!/bin/bash

./ucac perform --monitoring-namespace=ddebowski \
--peers=$(python3 -c "print('$1,' * $2, end=''); print('$1')") --test-namespace=ddebowski --application-image=ghcr.io/davenury/ucac:e6ee9ddfe6fd41f5f04acf21d3057a839ee692a7 \
--performance-test-image=ghcr.io/davenury/tests:e6ee9ddfe6fd41f5f04acf21d3057a839ee692a7 --constant-load=10 \
--fixed-peersets-in-change=2 --tests-sending-strategy=delay_on_conflicts \
--ac-protocol=gpac --enforce-ac --performance-test-timeout-deadline=PT120M --proxy-limit=0
