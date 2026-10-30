# Fabric / OneLake / Power BI REST API の共通関数（bash で source して使う）
#
# 前提: Azure CLI でサインイン済み（az login）。
# 社内プロキシやウイルス対策ソフトが HTTPS を検査していて Azure CLI が CERTIFICATE_VERIFY_FAILED になる場合は、
# その検査用ルート証明書を含む CA バンドルを REQUESTS_CA_BUNDLE に設定してから実行する（検証の無効化はしない）。

FABRIC_API=https://api.fabric.microsoft.com/v1
PBI_API=https://api.powerbi.com/v1.0/myorg
ONELAKE=https://onelake.dfs.fabric.microsoft.com

: "${WORK_DIR:?WORK_DIR を設定してください（deploy.sh が設定します）}"
mkdir -p "$WORK_DIR"
HEADERS_FILE="$WORK_DIR/last_headers.txt"

token_for() { az account get-access-token --resource "$1" --query accessToken -o tsv; }
fab_token() { token_for https://api.fabric.microsoft.com; }
onelake_token() { token_for https://storage.azure.com; }
pbi_token() { token_for https://analysis.windows.net/powerbi/api; }

log() { printf '[%s] %s\n' "$(date +%H:%M:%S)" "$*" >&2; }
die() { log "ERROR: $*"; exit 1; }

json_get() { node -e 'let d="";process.stdin.on("data",c=>d+=c).on("end",()=>{try{const v=process.argv[1].split(".").reduce((o,k)=>o==null?o:o[k],JSON.parse(d));console.log(v==null?"":typeof v==="object"?JSON.stringify(v):v)}catch(e){console.log("")}})' "$1"; }

# fab METHOD PATH [BODY_FILE]  -> 本文を標準出力へ。HTTP ステータスを FAB_STATUS に入れる
fab() {
  local method=$1 path=$2 body=${3:-} out
  local args=(-s -X "$method" -H "Authorization: Bearer $(fab_token)" -H "Content-Type: application/json" -D "$HEADERS_FILE" -w $'\n%{http_code}')
  [ -n "$body" ] && args+=(--data-binary "@$body")
  out=$(curl "${args[@]}" "$FABRIC_API$path") || die "curl failed: $method $path"
  FAB_STATUS=${out##*$'\n'}
  printf '%s\n' "${out%$'\n'*}"
}

last_location() { grep -i '^location:' "$HEADERS_FILE" | tr -d '\r' | awk '{print $2}'; }

# 長時間処理（202 Accepted）の完了を待ち、結果（作成された項目など）を出力する
wait_operation() {
  local op=$1 status r
  [ -z "$op" ] && return 0
  for _ in $(seq 1 60); do
    r=$(curl -s -H "Authorization: Bearer $(fab_token)" "$op")
    status=$(echo "$r" | json_get status)
    case "$status" in Succeeded) curl -s -H "Authorization: Bearer $(fab_token)" "$op/result"; return 0;; Failed) die "operation failed: $r";; esac
    sleep 4
  done
  die "operation timeout: $op"
}

# 項目の作成（同期 201 / 非同期 202 の両方に対応）。作成された項目の JSON を出力する
create_item() {
  local path=$1 body=$2 res
  res=$(fab POST "$path" "$body")
  case "$FAB_STATUS" in
    201) echo "$res" ;;
    202) wait_operation "$(last_location)" ;;
    *) die "create failed ($FAB_STATUS): $res" ;;
  esac
}

# ジョブ（Notebook / Pipeline）の実行と完了待ち。最終状態を出力する
run_job() {
  local item=$1 job_type=$2 body=$3 timeout=${4:-3600} job status r start
  fab POST "/workspaces/$WORKSPACE_ID/items/$item/jobs/instances?jobType=$job_type" "$body" >/dev/null
  [ "$FAB_STATUS" = 202 ] || die "job start failed ($FAB_STATUS)"
  job=$(last_location); start=$(date +%s)
  while true; do
    r=$(curl -s -H "Authorization: Bearer $(fab_token)" "$job")
    status=$(echo "$r" | json_get status)
    log "job $job_type: $status ($(( $(date +%s) - start ))s)"
    case "$status" in Completed) echo "$status"; return 0;; Failed|Cancelled|Deduped) echo "$r" >&2; return 1;; esac
    [ $(( $(date +%s) - start )) -ge "$timeout" ] && die "job timeout"
    sleep 30
  done
}

# OneLake の Lakehouse Files にローカルファイルを 1 つアップロードする（ADLS Gen2 DFS API）
onelake_upload() {
  local local_file=$1 remote_path=$2 tok len url
  tok=$(onelake_token); len=$(stat -c %s "$local_file")
  url="$ONELAKE/$WORKSPACE_ID/$LAKEHOUSE_ID/Files/$remote_path"
  local h=(-s -o /dev/null -w "%{http_code}" -H "Authorization: Bearer $tok" -H "x-ms-version: 2023-11-03")
  [ "$(curl "${h[@]}" -X PUT -H "Content-Length: 0" "$url?resource=file")" = 201 ] || die "create failed: $remote_path"
  [ "$(curl "${h[@]}" -X PATCH -H "Content-Type: application/octet-stream" --data-binary "@$local_file" "$url?action=append&position=0")" = 202 ] || die "append failed: $remote_path"
  [ "$(curl "${h[@]}" -X PATCH -H "Content-Length: 0" "$url?action=flush&position=$len")" = 200 ] || die "flush failed: $remote_path"
}

onelake_download() {
  curl -s -H "Authorization: Bearer $(onelake_token)" -H "x-ms-version: 2023-11-03" "$ONELAKE/$WORKSPACE_ID/$LAKEHOUSE_ID/Files/$1"
}

# pbi METHOD PATH [BODY_FILE]
pbi() {
  local method=$1 path=$2 body=${3:-}
  local args=(-s -X "$method" -H "Authorization: Bearer $(pbi_token)" -H "Content-Type: application/json; charset=utf-8")
  [ -n "$body" ] && args+=(--data-binary "@$body")
  curl "${args[@]}" "$PBI_API$path"
}

# 状態ファイル（作成した項目の ID）を保存・読み込みする
state_set() { local k=$1 v=$2 f="$WORK_DIR/state.env"; touch "$f"; grep -v "^$k=" "$f" > "$f.tmp" || true; echo "$k=$v" >> "$f.tmp"; mv "$f.tmp" "$f"; export "$k=$v"; }
state_load() { [ -f "$WORK_DIR/state.env" ] && set -a && . "$WORK_DIR/state.env" && set +a; return 0; }
