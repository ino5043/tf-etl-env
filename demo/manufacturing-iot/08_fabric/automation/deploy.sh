#!/usr/bin/env bash
# 製造業 IoT デモを Microsoft Fabric に自動配置する（deploy-guide.md 手順 2〜9 の自動化）
#
# Usage:
#   cp config.example.sh config.sh   # 値を編集する
#   ./deploy.sh all                  # すべて実行
#   ./deploy.sh <step> [<step> ...]  # 手順を選んで実行
#
# 手順: workspace lakehouse upload notebook run verify model report pipeline shots
# 作成した項目の ID は $WORK_DIR/state.env に保存され、後続の手順や再実行で使われる。
set -Eeuo pipefail
# set -e で止まったときに、止まった行とコマンドを必ず表示する（黙って終了しないように）
trap 'echo "[$(date +%H:%M:%S)] ERROR: line $LINENO: $BASH_COMMAND (exit $?)" >&2' ERR

HERE=$(cd "$(dirname "$0")" && pwd)
FABRIC_DIR=$(cd "$HERE/.." && pwd)
CONFIG=${CONFIG:-$HERE/config.sh}
[ -f "$CONFIG" ] || { echo "設定ファイルがありません: $CONFIG（config.example.sh をコピーして作成）" >&2; exit 1; }
# shellcheck source=/dev/null
. "$CONFIG"
export WORK_DIR=${WORK_DIR:-$HERE/.work}
# shellcheck source=lib/fabric.sh
. "$HERE/lib/fabric.sh"
state_load

: "${CAPACITY_ID:?config.sh に CAPACITY_ID を設定してください}"
WORKSPACE_NAME=${WORKSPACE_NAME:-factory-etl-demo}
LAKEHOUSE_NAME=${LAKEHOUSE_NAME:-factory_lakehouse}
MODEL_NAME=${MODEL_NAME:-工場生産ダッシュボード}
REPORT_NAME=${REPORT_NAME:-工場長 朝会レポート}
PIPELINE_NAME=${PIPELINE_NAME:-pl_daily_production_load}
TARGET_DATE=${TARGET_DATE:-2026-10-30}
DEMO_FROM=${DEMO_FROM:-2026-10-01}
DEMO_TO=${DEMO_TO:-2026-10-31}

need() { [ -n "${!1:-}" ] || die "$1 がありません。先に '$2' を実行してください"; }

# 表示名で既存の項目を探す（再実行時に重複作成しないため）
find_item() { fab GET "/workspaces/$WORKSPACE_ID/items?type=$1" | node -e 'let d="";process.stdin.on("data",c=>d+=c).on("end",()=>{const v=(JSON.parse(d).value||[]).find(i=>i.displayName===process.argv[1]);console.log(v?v.id:"")})' "$2"; }

# 既定の Lakehouse を指定して Notebook を実行するためのリクエスト本文
notebook_run_body() {
  printf '{"executionData":{"configuration":{"defaultLakehouse":{"name":"%s","id":"%s","workspaceId":"%s"}}}}' "$LAKEHOUSE_NAME" "$LAKEHOUSE_ID" "$WORKSPACE_ID" > "$WORK_DIR/run_body.json"
  echo "$WORK_DIR/run_body.json"
}

# Notebook を作成（同名があれば定義を更新）し、ID を出力する
upsert_notebook() {
  local src=$1 name=$2 id body="$WORK_DIR/nb_$2.json"
  node "$HERE/scripts/make_notebook_body.js" "$src" "$name" "$WORKSPACE_ID" "$LAKEHOUSE_ID" "$LAKEHOUSE_NAME" "$body"
  id=$(find_item Notebook "$name")
  if [ -n "$id" ]; then
    node -e 'const fs=require("fs");const b=JSON.parse(fs.readFileSync(process.argv[1]));fs.writeFileSync(process.argv[1],JSON.stringify({definition:b.definition}))' "$body"
    fab POST "/workspaces/$WORKSPACE_ID/notebooks/$id/updateDefinition" "$body" >/dev/null
    [ "$FAB_STATUS" = 202 ] && wait_operation "$(last_location)" >/dev/null
    echo "$id"
  else
    create_item "/workspaces/$WORKSPACE_ID/notebooks" "$body" | json_get id
  fi
}

# Python コードを一時 Notebook として実行し、Files/_verify/<name>.json を取得する
run_code() {
  local name=$1 code=$2 out=$3 id
  id=$(upsert_notebook "$code" "$name")
  run_job "$id" RunNotebook "$(notebook_run_body)" 1800 >/dev/null || die "$name failed"
  onelake_download "_verify/$name.json" > "$out"
}

step_workspace() {
  local id; id=$(fab GET /workspaces | node -e 'let d="";process.stdin.on("data",c=>d+=c).on("end",()=>{const v=(JSON.parse(d).value||[]).find(w=>w.displayName===process.argv[1]);console.log(v?v.id:"")})' "$WORKSPACE_NAME")
  if [ -z "$id" ]; then
    printf '{"displayName":"%s","description":"Manufacturing IoT ETL demo (tf-etl-env)","capacityId":"%s"}' "$WORKSPACE_NAME" "$CAPACITY_ID" > "$WORK_DIR/ws.json"
    id=$(create_item /workspaces "$WORK_DIR/ws.json" | json_get id)
  fi
  state_set WORKSPACE_ID "$id"; log "workspace: $WORKSPACE_NAME ($id)"
}

step_lakehouse() {
  need WORKSPACE_ID workspace
  local id; id=$(find_item Lakehouse "$LAKEHOUSE_NAME")
  if [ -z "$id" ]; then
    printf '{"displayName":"%s","creationPayload":{"enableSchemas":true}}' "$LAKEHOUSE_NAME" > "$WORK_DIR/lh.json"
    id=$(create_item "/workspaces/$WORKSPACE_ID/lakehouses" "$WORK_DIR/lh.json" | json_get id)
  fi
  state_set LAKEHOUSE_ID "$id"; log "lakehouse: $LAKEHOUSE_NAME ($id)"
}

step_upload() {
  need LAKEHOUSE_ID lakehouse
  local n=0
  for f in "$FABRIC_DIR"/sample_data/*.csv; do onelake_upload "$f" "sample_data/$(basename "$f")"; n=$((n + 1)); done
  log "uploaded $n files to Files/sample_data"
}

step_notebook() {
  need LAKEHOUSE_ID lakehouse
  state_set NOTEBOOK_ID "$(upsert_notebook "$FABRIC_DIR/notebook/fabric_pipeline.ipynb" fabric_pipeline)"
  log "notebook: fabric_pipeline ($NOTEBOOK_ID)"
}

step_run() {
  need NOTEBOOK_ID notebook
  run_job "$NOTEBOOK_ID" RunNotebook "$(notebook_run_body)" 3600 >/dev/null || die "fabric_pipeline failed"
}

step_verify() {
  need LAKEHOUSE_ID lakehouse
  run_code verify_pipeline "$HERE/notebooks/verify_pipeline.py" "$WORK_DIR/verify.json"
  node "$HERE/scripts/check_verify.js" "$WORK_DIR/verify.json"
}

step_model() {
  need LAKEHOUSE_ID lakehouse
  run_code schema_dump "$HERE/notebooks/schema_dump.py" "$WORK_DIR/schema.json"
  local lh sql_server sql_ep id body="$WORK_DIR/model.json"
  lh=$(fab GET "/workspaces/$WORKSPACE_ID/lakehouses/$LAKEHOUSE_ID")
  sql_server=$(echo "$lh" | json_get properties.sqlEndpointProperties.connectionString)
  sql_ep=$(echo "$lh" | json_get properties.sqlEndpointProperties.id)
  [ -n "$sql_ep" ] || die "SQL 分析エンドポイントの準備ができていません。しばらく待って再実行してください"
  # Notebook 直後は SQL エンドポイント側のテーブル同期が遅れることがあるため、明示的に同期する
  echo '{}' > "$WORK_DIR/empty.json"
  fab POST "/workspaces/$WORKSPACE_ID/sqlEndpoints/$sql_ep/refreshMetadata" "$WORK_DIR/empty.json" >/dev/null || true
  [ "$FAB_STATUS" = 202 ] && wait_operation "$(last_location)" >/dev/null
  node "$HERE/scripts/build_model.js" "$FABRIC_DIR/powerbi" "$WORK_DIR/schema.json" "$sql_server" "$sql_ep" "$MODEL_NAME" "$body"
  id=$(find_item SemanticModel "$MODEL_NAME")
  if [ -n "$id" ]; then
    node -e 'const fs=require("fs");const b=JSON.parse(fs.readFileSync(process.argv[1]));fs.writeFileSync(process.argv[1]+".update",JSON.stringify({definition:b.definition}))' "$body"
    fab POST "/workspaces/$WORKSPACE_ID/semanticModels/$id/updateDefinition" "$body.update" >/dev/null
    [ "$FAB_STATUS" = 202 ] && wait_operation "$(last_location)" >/dev/null
  else
    id=$(create_item "/workspaces/$WORKSPACE_ID/semanticModels" "$body" | json_get id)
  fi
  state_set MODEL_ID "$id"; log "semantic model: $MODEL_NAME ($id)"
  # Direct Lake のフレーミング（更新）
  echo '{"type":"full"}' > "$WORK_DIR/refresh.json"
  pbi POST "/groups/$WORKSPACE_ID/datasets/$id/refreshes" "$WORK_DIR/refresh.json" >/dev/null
  local st=""
  for _ in $(seq 1 60); do
    sleep 5
    st=$(pbi GET "/groups/$WORKSPACE_ID/datasets/$id/refreshes?\$top=1" | json_get value.0.status)
    case "$st" in Completed|Failed) break;; esac
  done
  log "model refresh: $st"
  [ "$st" = Completed ] || die "model refresh failed（SQL エンドポイントの同期後に 'model' を再実行してください）"
  PBI_TOKEN=$(pbi_token) node "$HERE/scripts/check_all_measures.js" "$WORKSPACE_ID" "$id" "$WORK_DIR/measure_tables.json"
}

step_report() {
  need MODEL_ID model
  local id body="$WORK_DIR/report.json"
  node "$HERE/scripts/build_report.js" "$MODEL_ID" "$WORK_DIR/measure_tables.json" "$REPORT_NAME" "$DEMO_FROM" "$DEMO_TO" "$body"
  id=$(find_item Report "$REPORT_NAME")
  if [ -n "$id" ]; then
    node -e 'const fs=require("fs");const b=JSON.parse(fs.readFileSync(process.argv[1]));fs.writeFileSync(process.argv[1]+".update",JSON.stringify({definition:b.definition}))' "$body"
    fab POST "/workspaces/$WORKSPACE_ID/reports/$id/updateDefinition" "$body.update" >/dev/null
    [ "$FAB_STATUS" = 202 ] && wait_operation "$(last_location)" >/dev/null
  else
    id=$(create_item "/workspaces/$WORKSPACE_ID/reports" "$body" | json_get id)
  fi
  state_set REPORT_ID "$id"; log "report: $REPORT_NAME ($id)"
}

step_pipeline() {
  need NOTEBOOK_ID notebook
  local id body="$WORK_DIR/pipeline.json"
  node "$HERE/scripts/build_pipeline.js" "$PIPELINE_NAME" "$WORKSPACE_ID" "$NOTEBOOK_ID" "$TARGET_DATE" "$body"
  id=$(find_item DataPipeline "$PIPELINE_NAME")
  [ -n "$id" ] || id=$(create_item "/workspaces/$WORKSPACE_ID/dataPipelines" "$body" | json_get id)
  state_set PIPELINE_ID "$id"; log "pipeline: $PIPELINE_NAME ($id)"
  printf '{"executionData":{"parameters":{"target_date":"%s"}}}' "$TARGET_DATE" > "$WORK_DIR/pl_run.json"
  run_job "$id" Pipeline "$WORK_DIR/pl_run.json" 3600 >/dev/null || die "pipeline failed"
}

step_shots() {
  need REPORT_ID report
  PBI_TOKEN=$(pbi_token) node "$HERE/scripts/shoot_report.js" "$WORKSPACE_ID" "$REPORT_ID" "$WORK_DIR/shots" p1 p2 p3
}

ALL=(workspace lakehouse upload notebook run verify model report pipeline shots)
[ $# -gt 0 ] || { sed -n '2,11p' "$0"; exit 1; }
[ "$1" = all ] && set -- "${ALL[@]}"
for s in "$@"; do
  declare -F "step_$s" >/dev/null || die "unknown step: $s（${ALL[*]}）"
  log "=== $s ==="
  "step_$s"
done
log "done. state: $WORK_DIR/state.env"
