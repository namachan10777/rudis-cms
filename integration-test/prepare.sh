#!/bin/sh

set -eux

cargo run -- --config ./integration-test/config.yaml show-schema typescript --save integration-test/generated --valibot
