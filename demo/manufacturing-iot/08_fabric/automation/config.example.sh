# deploy.sh の設定。config.sh にコピーして値を入れる（config.sh は Git の管理対象外）
#
# 容量 ID の調べ方（トライアル容量は displayName が "Trial-..."）:
#   az account get-access-token --resource https://api.fabric.microsoft.com --query accessToken -o tsv \
#     | xargs -I{} curl -s -H "Authorization: Bearer {}" https://api.fabric.microsoft.com/v1/capacities

CAPACITY_ID=""                       # 必須: ワークスペースを割り当てる容量の ID

WORKSPACE_NAME="factory-etl-demo"
LAKEHOUSE_NAME="factory_lakehouse"
MODEL_NAME="工場生産ダッシュボード"
REPORT_NAME="工場長 朝会レポート"
PIPELINE_NAME="pl_daily_production_load"

TARGET_DATE="2026-10-30"             # パイプラインで処理する日（サンプルデータは 2026-10-01〜30）
DEMO_FROM="2026-10-01"               # レポート全体の期間フィルター（以上）
DEMO_TO="2026-10-31"                 # レポート全体の期間フィルター（未満）

# ウイルス対策ソフトやプロキシが HTTPS を検査していて Azure CLI が証明書エラーになる場合のみ設定する
# export REQUESTS_CA_BUNDLE="/path/to/ca-bundle-with-inspection-root.pem"
