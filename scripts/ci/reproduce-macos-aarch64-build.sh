#!/bin/bash

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

set -euo pipefail

readonly REPOSITORY_URL="https://github.com/adamrecsko/iggy.git"
readonly REPOSITORY_BRANCH="feat/connectors-fluss-connector"
readonly EXPECTED_COMMIT="af17922c4c697cfdce15afec08226e9e98e8ab19"
readonly CHECKOUT_DIR="/tmp/iggy-macos-aarch64-reproduction"

echo "macOS: $(sw_vers -productVersion)"
echo "architecture: $(uname -m)"

if [[ -x /opt/homebrew/bin/brew ]]; then
    eval "$(/opt/homebrew/bin/brew shellenv)"
elif command -v brew >/dev/null 2>&1; then
    eval "$(brew shellenv)"
else
    readonly HOMEBREW_INSTALLER="/tmp/install-homebrew.sh"
    curl -fsSL \
        https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh \
        -o "${HOMEBREW_INSTALLER}"
    NONINTERACTIVE=1 /bin/bash "${HOMEBREW_INSTALLER}"
    eval "$(/opt/homebrew/bin/brew shellenv)"
fi
export HOMEBREW_NO_AUTO_UPDATE=1

git clone \
    --branch "${REPOSITORY_BRANCH}" \
    --filter=blob:none \
    --single-branch \
    "${REPOSITORY_URL}" \
    "${CHECKOUT_DIR}"
cd "${CHECKOUT_DIR}"

ACTUAL_COMMIT="$(git rev-parse HEAD)"
readonly ACTUAL_COMMIT
if [[ "${ACTUAL_COMMIT}" != "${EXPECTED_COMMIT}" ]]; then
    echo "Expected commit ${EXPECTED_COMMIT}, got ${ACTUAL_COMMIT}" >&2
    exit 1
fi

# Mirror .github/actions/utils/setup-rust-with-cache/action.yml.
brew tap-new iggy/local-hwloc </dev/null
curl -fsSL \
    https://raw.githubusercontent.com/Homebrew/homebrew-core/bb1e23f8e5eacf4d31acd489f6079c8a53ebd690/Formula/h/hwloc.rb \
    -o "$(brew --repository iggy/local-hwloc)/Formula/hwloc.rb"
brew install iggy/local-hwloc/hwloc </dev/null
if ! command -v pkg-config >/dev/null 2>&1; then
    brew install pkgconf </dev/null
fi

if ! command -v rustup >/dev/null 2>&1; then
    brew install rustup </dev/null
fi
export PATH="/opt/homebrew/opt/rustup/bin:${HOME}/.cargo/bin:${PATH}"
if command -v rustup-init >/dev/null 2>&1; then
    rustup-init -y --no-modify-path --default-toolchain none </dev/null
fi
export CARGO_INCREMENTAL=0
export CARGO_PROFILE_DEV_DEBUG=0
export CARGO_PROFILE_TEST_DEBUG=0
export CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse

rustup show

if command -v protoc >/dev/null 2>&1; then
    echo "protoc is unexpectedly installed: $(command -v protoc)" >&2
    protoc --version >&2
    exit 1
fi

cargo build --locked
