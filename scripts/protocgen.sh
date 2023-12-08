#!/usr/bin/env bash

set -eo pipefail

buf generate --path proto/iavl

mv ./proto/iavl/*.go ./proto

buf generate --path proto/memiavl

mv ./proto/memiavl/*.go ./proto