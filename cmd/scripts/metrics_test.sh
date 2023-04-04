#!/bin/bash
SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &> /dev/null && pwd)
"${SCRIPT_DIR}/../ucac" perform --monitoring-namespace=ddebowski \
--peers=3,3 --test-namespace=ddebowski --application-image=ghcr.io/davenury/ucac:9f44a32df124d248718273c70399c924ffaf2064 \
--performance-test-image=ghcr.io/davenury/tests:9f44a32df124d248718273c70399c924ffaf2064 --fixed-peersets-in-change=2 --tests-sending-strategy=delay_on_conflicts \
--ac-protocol=gpac --performance-test-timeout-deadline=PT120M --is-metric-test --constant-load=1 --load-generator-type=constant --tests-creating-change-strategy=processable_conflicts
