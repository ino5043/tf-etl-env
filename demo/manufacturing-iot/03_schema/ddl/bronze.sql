-- =============================================================================
-- Bronze Layer DDL: raw_production
-- =============================================================================
-- Platform:    Azure Fabric Lakehouse (Delta Lake / Spark SQL)
-- Purpose:     CSV 生データの忠実な保存（全列 STRING）
-- Partition:   _ingested_date (取り込み日)
-- Idempotency: パーティション単位の上書き（REPLACE WHERE）
-- =============================================================================

-- スキーマ作成（存在しない場合）
CREATE SCHEMA IF NOT EXISTS bronze;

-- -----------------------------------------------------------------------------
-- raw_production: 工場生産実績の生データ
-- -----------------------------------------------------------------------------
-- CSV ファイルから取り込んだデータをそのまま格納する。
-- 全列を STRING 型とし、型変換エラーによるデータ欠損を防止する。
-- メタデータ列（_ingested_at, _source_file, _batch_id）でリネージを追跡する。
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS bronze.raw_production (
    -- ソースデータ列（全 STRING）
    production_date     STRING      COMMENT '生産日（ソース値そのまま）',
    line_id             STRING      COMMENT '生産ライン ID（LINE-001〜LINE-005）',
    product_code        STRING      COMMENT '製品コード（PRD-A001〜PRD-A020）',
    planned_quantity    STRING      COMMENT '計画数量',
    actual_quantity     STRING      COMMENT '実績数量',
    defect_quantity     STRING      COMMENT '不良数量',
    equipment_id        STRING      COMMENT '設備 ID（EQ-001〜EQ-010）',
    shift               STRING      COMMENT 'シフト（日勤/夜勤）',
    operator_id         STRING      COMMENT '作業者 ID（OP-001〜OP-050）',
    temperature         STRING      COMMENT '温度（℃）',
    vibration           STRING      COMMENT '振動（mm/s）',
    status              STRING      COMMENT '設備状態（正常/警告/異常/停止）',
    created_at          STRING      COMMENT 'レコード作成日時（ソース値そのまま）',

    -- メタデータ列（パイプラインが付与）
    _ingested_at        TIMESTAMP   COMMENT '取り込み日時（パイプライン実行時刻）',
    _source_file        STRING      COMMENT '取り込み元ファイル名（例: production_20260315.csv）',
    _batch_id           STRING      COMMENT 'バッチ実行 ID（UUID）',

    -- パーティション列（_ingested_at から導出）
    _ingested_date      DATE        COMMENT '取り込み日（パーティションキー）'
)
USING DELTA
PARTITIONED BY (_ingested_date)
COMMENT '工場生産実績の生データ（Bronze レイヤー）。CSV から全列 STRING で取り込み。'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true',
    'delta.logRetentionDuration'       = 'interval 30 days',
    'delta.deletedFileRetentionDuration' = 'interval 7 days'
);
