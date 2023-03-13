#!/bin/bash

./ucac perform --monitoring-namespace=ddebowski \
--peers=$(python3 -c "print('$1,' * $2, end=''); print('$1')") --test-namespace=ddebowski --application-image=ghcr.io/davenury/ucac:91177497ed1198bfe5b036af9ee95e18a8b1bdb6 \
--performance-test-image=ghcr.io/davenury/tests:91177497ed1198bfe5b036af9ee95e18a8b1bdb6 --constant-load=5 \
--fixed-peersets-in-change=2 --tests-sending-strategy=delay_on_conflicts \
--ac-protocol=gpac --enforce-ac --performance-test-timeout-deadline=PT120M --proxy-limit=0
