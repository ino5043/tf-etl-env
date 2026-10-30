# deploy.js（./deploy.sh）の設定。config.sh にコピーして値を入れる（config.sh は Git の管理対象外）
#
# このファイルはシェルとして実行されない。KEY="value" / KEY='value' / KEY=value の行（先頭の export 可）と
# # で始まるコメントだけが書ける。$VAR や $(...) の展開はしない（書くとエラーになる）。
# 同じ名前の環境変数があれば、そちらが優先される（例: WORKSPACE_NAME=demo2 ./deploy.sh workspace）。
#
# 容量 ID の調べ方（トライアル容量は displayName が "Trial-..."）:
#   az rest --method get --url https://api.fabric.microsoft.com/v1/capacities --resource https://api.fabric.microsoft.com

CAPACITY_ID=""                       # 必須: ワークスペースを割り当てる容量の ID（GUID）

WORKSPACE_NAME="factory-etl-demo"
LAKEHOUSE_NAME="factory_lakehouse"   # 英字で始まる英数字と _ のみ
MODEL_NAME="工場生産ダッシュボード"
REPORT_NAME="工場長 朝会レポート"
PIPELINE_NAME="pl_daily_production_load"

TARGET_DATE="2026-10-30"             # パイプラインで処理する日（サンプルデータは 2026-10-01〜30）
DEMO_FROM="2026-10-01"               # レポート全体の期間フィルター（以上）
DEMO_TO="2026-10-31"                 # レポート全体の期間フィルター（未満）

# 作業フォルダー（ID・API 本文・画面キャプチャを保存する）。既定は automation/.work。
# リポジトリが OneDrive 等の同期フォルダーにある場合は、同期されない場所を指定するとよい
# WORK_DIR="C:/work/fabric-demo"

# ウイルス対策ソフトやプロキシが HTTPS を検査していて Azure CLI が証明書エラーになる場合のみ設定する
# export REQUESTS_CA_BUNDLE="/path/to/ca-bundle-with-inspection-root.pem"
