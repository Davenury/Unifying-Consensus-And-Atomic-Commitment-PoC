#!/bin/bash

./ucac perform --monitoring-namespace=ddebowski \
--peers=$(python3 -c "print('$1,' * $2, end=''); print('$1')") --test-namespace=ddebowski --application-image=ghcr.io/davenury/ucac:bc30be1cb0e06873f28195104ace6bd8b1a24232  \
--performance-test-image=ghcr.io/davenury/tests:bc30be1cb0e06873f28195104ace6bd8b1a24232 \
--fixed-peersets-in-change=1 --tests-sending-strategy=delay_on_conflicts \
--ac-protocol=two_pc --performance-test-timeout-deadline=PT120M --deploy-monitoring=false \
--load-generator-type=increasing --load-bound=1000 --load-increase-delay=PT20S --load-increase-step=5
