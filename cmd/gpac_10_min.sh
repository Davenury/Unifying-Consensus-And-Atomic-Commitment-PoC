#!/bin/bash

./ucac perform --monitoring-namespace=ddebowski \
--peers=$(python3 -c "print('$1,' * $2, end=''); print('$1')") --test-namespace=ddebowski --application-image=ghcr.io/davenury/ucac:d77c3bf18df5bf67ede7ebe032b458c67362d3f1 \
--performance-test-image=ghcr.io/davenury/tests:d77c3bf18df5bf67ede7ebe032b458c67362d3f1 --constant-load=7 \
--fixed-peersets-in-change=2 --tests-sending-strategy=delay_on_conflicts \
--ac-protocol=gpac --enforce-ac --performance-test-timeout-deadline=PT120M --proxy-limit=0
