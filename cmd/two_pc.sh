#!/bin/bash

./ucac perform --monitoring-namespace=ddebowski \
--peers=$(python3 -c "print('$1,' * $2, end=''); print('$1')") --test-namespace=kjarosz --application-image=ghcr.io/davenury/ucac:9131551bb2368caa9cdd2f933bead63b7860589c \
--performance-test-image=ghcr.io/davenury/tests:9131551bb2368caa9cdd2f933bead63b7860589c --constant-load=7 \
--fixed-peersets-in-change=2 --tests-sending-strategy=delay_on_conflicts \
--ac-protocol=two_pc --enforce-ac --performance-test-timeout-deadline=PT120M --proxy-limit=0 --deploy-monitoring=false
