#!/bin/bash

./ucac perform --monitoring-namespace=ddebowski \
--peers=$(python3 -c "print('$1,' * $2, end=''); print('$1')") --test-namespace=ddebowski --application-image=ghcr.io/davenury/ucac:d658f56606e3cb2fcfca4ad534185d9e4194f8e6 \
--performance-test-image=ghcr.io/davenury/tests:d658f56606e3cb2fcfca4ad534185d9e4194f8e6 --constant-load=7 \
--fixed-peersets-in-change=2 --tests-sending-strategy=delay_on_conflicts \
--ac-protocol=gpac --enforce-ac --performance-test-timeout-deadline=PT120M --proxy-limit=0
