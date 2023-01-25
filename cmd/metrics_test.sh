#!/bin/bash

./ucac perform --monitoring-namespace=ddebowski \
--peers=3,3 --test-namespace=ddebowski --application-image=ghcr.io/davenury/ucac:71bfdd0a4112b74c95ba429f4b38c1f979c28fa1 \
--performance-test-image=ghcr.io/davenury/tests:71bfdd0a4112b74c95ba429f4b38c1f979c28fa1  --max-peersets-in-change=2 --tests-strategy=delay_on_conflicts \
--ac-protocol=gpac --performance-test-timeout-deadline=PT120M --is-metric-test --constant-load=1 --deploy-monitoring=false
