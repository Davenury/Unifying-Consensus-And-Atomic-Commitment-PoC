#!/bin/bash

./ucac perform --monitoring-namespace=ddebowski \
--peers=$(python3 -c "print('$1,' * $2, end=''); print('$1')") --test-namespace=ddebowski --application-image=ghcr.io/davenury/ucac:28830134d5546d757fdb86d68e8a0f4f798c4770 \
--performance-test-image=ghcr.io/davenury/tests:28830134d5546d757fdb86d68e8a0f4f798c4770 --constant-load=7 \
--fixed-peersets-in-change=2 --tests-sending-strategy=delay_on_conflicts \
--ac-protocol=gpac --enforce-ac --performance-test-timeout-deadline=PT120M --proxy-limit=0
