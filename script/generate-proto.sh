#!/usr/bin/env bash

set -euo pipefail

repository_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
easytier_source="${EASYTIER_SOURCE:-"${repository_root}/../EasyTier"}"
proto_root="${easytier_source}/easytier-proto/proto"

if [[ ! -f "${proto_root}/api_instance.proto" ]]; then
    echo "EasyTier proto source not found at ${proto_root}" >&2
    exit 1
fi

protoc \
    -I "${proto_root}" \
    --go_out="${repository_root}" \
    --go_opt=module=github.com/EasyTier/easytier-go-host \
    --go_opt=Mcommon.proto=github.com/EasyTier/easytier-go-host/proto/common \
    --go_opt=Merror.proto=github.com/EasyTier/easytier-go-host/proto/error \
    --go_opt=Macl.proto=github.com/EasyTier/easytier-go-host/proto/acl \
    --go_opt=Mpeer_rpc.proto=github.com/EasyTier/easytier-go-host/proto/peer_rpc \
    --go_opt=Mapi_instance.proto=github.com/EasyTier/easytier-go-host/proto/api/instance \
    "${proto_root}/common.proto" \
    "${proto_root}/error.proto" \
    "${proto_root}/acl.proto" \
    "${proto_root}/peer_rpc.proto" \
    "${proto_root}/api_instance.proto"
