#!/usr/bin/env bash
# 互換用のラッパー: 処理本体は deploy.js（Node.js）。使い方は README.md を参照
#   ./deploy.sh all | <step> [<step> ...]
# --use-system-ca: OS の証明書ストアを信頼する（HTTPS 検査をするウイルス対策ソフト・プロキシの環境向け。curl 版と同じ挙動）
set -euo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
exec node --use-system-ca "$HERE/deploy.js" "$@"
