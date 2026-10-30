-- =============================================================================
-- Gold Layer DDL: daily_production_kpi, equipment_health
-- =============================================================================
-- Platform:    Azure Fabric Lakehouse (Delta Lake / Spark SQL)
-- Purpose:     BI 向け事前集計テーブル（Star Schema）
-- Partition:   production_date (生産日)
-- Idempotency: DELETE + INSERT by production_date
-- =============================================================================

-- スキーマ作成（存在しない場合）
CREATE SCHEMA IF NOT EXISTS gold;

-- -----------------------------------------------------------------------------
-- daily_production_kpi: 日次生産 KPI（ライン別集計）
-- -----------------------------------------------------------------------------
-- silver.production_facts を日次 x ライン粒度で集計したファクトテーブル。
-- 工場長ダッシュボードの主要データソース。
-- Star Schema のファクトテーブルとして設計。
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS gold.daily_production_kpi (
    -- ディメンションキー（複合主キー）
    production_date         DATE        NOT NULL    COMMENT '生産日',
    line_id                 STRING      NOT NULL    COMMENT '生産ライン ID',

    -- 数量 KPI
    total_planned_quantity  BIGINT      NOT NULL    COMMENT '計画数量合計',
    total_actual_quantity   BIGINT      NOT NULL    COMMENT '実績数量合計',
    total_defect_quantity   BIGINT      NOT NULL    COMMENT '不良数量合計',
    total_good_quantity     BIGINT      NOT NULL    COMMENT '良品数量合計',

    -- 比率 KPI
    achievement_rate        DOUBLE      NOT NULL    COMMENT '達成率（実績合計 / 計画合計）',
    defect_rate             DOUBLE      NOT NULL    COMMENT '不良率（不良合計 / 実績合計）',
    yield_rate              DOUBLE      NOT NULL    COMMENT '歩留まり率（良品合計 / 実績合計）',

    -- 補助メトリクス
    record_count            INT         NOT NULL    COMMENT '集計元レコード数',
    unique_products         INT         NOT NULL    COMMENT '製造品目数（DISTINCT product_code）',
    unique_operators        INT         NOT NULL    COMMENT '作業者数（DISTINCT operator_id）',

    -- シフト別数量
    day_shift_quantity      BIGINT      NOT NULL    COMMENT '日勤の実績数量',
    night_shift_quantity    BIGINT      NOT NULL    COMMENT '夜勤の実績数量',

    -- メタデータ
    _aggregated_at          TIMESTAMP   NOT NULL    COMMENT '集計実行日時'
)
USING DELTA
PARTITIONED BY (production_date)
COMMENT '日次生産 KPI（Gold レイヤー）。日次 x ライン粒度で集計。工場長ダッシュボード用。'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true',
    'delta.logRetentionDuration'       = 'interval 30 days',
    'delta.deletedFileRetentionDuration' = 'interval 7 days'
);

-- -----------------------------------------------------------------------------
-- daily_production_kpi: CHECK 制約
-- -----------------------------------------------------------------------------

ALTER TABLE gold.daily_production_kpi ADD CONSTRAINT chk_kpi_planned_positive
    CHECK (total_planned_quantity > 0);

ALTER TABLE gold.daily_production_kpi ADD CONSTRAINT chk_kpi_actual_non_negative
    CHECK (total_actual_quantity >= 0);

ALTER TABLE gold.daily_production_kpi ADD CONSTRAINT chk_kpi_defect_non_negative
    CHECK (total_defect_quantity >= 0);

ALTER TABLE gold.daily_production_kpi ADD CONSTRAINT chk_kpi_good_calculation
    CHECK (total_good_quantity = total_actual_quantity - total_defect_quantity);

ALTER TABLE gold.daily_production_kpi ADD CONSTRAINT chk_kpi_achievement_rate
    CHECK (achievement_rate >= 0.0);

ALTER TABLE gold.daily_production_kpi ADD CONSTRAINT chk_kpi_defect_rate
    CHECK (defect_rate >= 0.0 AND defect_rate <= 1.0);

ALTER TABLE gold.daily_production_kpi ADD CONSTRAINT chk_kpi_yield_rate
    CHECK (yield_rate >= 0.0 AND yield_rate <= 1.0);

ALTER TABLE gold.daily_production_kpi ADD CONSTRAINT chk_kpi_shift_sum
    CHECK (day_shift_quantity + night_shift_quantity = total_actual_quantity);

-- -----------------------------------------------------------------------------
-- equipment_health: 設備健全性（7日間移動平均）
-- -----------------------------------------------------------------------------
-- silver.production_facts を日次 x 設備粒度で集計し、
-- 7日間の移動平均でトレンドを追跡するテーブル。
-- 設備保全チームの予兆保全ダッシュボード用。
-- アラートフラグで閾値超過を事前判定する。
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS gold.equipment_health (
    -- ディメンションキー（複合主キー）
    production_date             DATE        NOT NULL    COMMENT '対象日',
    equipment_id                STRING      NOT NULL    COMMENT '設備 ID',
    line_id                     STRING      NOT NULL    COMMENT '設置ライン ID',

    -- 当日メトリクス
    daily_avg_temperature       DOUBLE                  COMMENT '当日平均温度（℃）',
    daily_max_temperature       DOUBLE                  COMMENT '当日最高温度（℃）',
    daily_avg_vibration         DOUBLE                  COMMENT '当日平均振動（mm/s）',
    daily_max_vibration         DOUBLE                  COMMENT '当日最大振動（mm/s）',

    -- 7日間移動平均
    rolling_7d_avg_temperature  DOUBLE                  COMMENT '7日間移動平均温度（℃）',
    rolling_7d_avg_vibration    DOUBLE                  COMMENT '7日間移動平均振動（mm/s）',

    -- アラートフラグ
    temperature_alert           BOOLEAN     NOT NULL    COMMENT '温度アラート（7日平均 > 65℃）',
    vibration_alert             BOOLEAN     NOT NULL    COMMENT '振動アラート（7日平均 > 3.5 mm/s）',

    -- 状態サマリ
    status_summary              STRING      NOT NULL    COMMENT '当日の状態サマリ（最も深刻な状態）',
    abnormal_count              INT         NOT NULL    COMMENT '当日の異常/停止発生回数',
    measurement_count           INT         NOT NULL    COMMENT '当日の計測レコード数',

    -- メタデータ
    _aggregated_at              TIMESTAMP   NOT NULL    COMMENT '集計実行日時'
)
USING DELTA
PARTITIONED BY (production_date)
COMMENT '設備健全性（Gold レイヤー）。7日間移動平均で温度・振動トレンドを追跡。予兆保全用。'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true',
    'delta.logRetentionDuration'       = 'interval 30 days',
    'delta.deletedFileRetentionDuration' = 'interval 7 days'
);

-- -----------------------------------------------------------------------------
-- equipment_health: CHECK 制約
-- -----------------------------------------------------------------------------

ALTER TABLE gold.equipment_health ADD CONSTRAINT chk_health_temperature_range
    CHECK (daily_avg_temperature IS NULL OR (daily_avg_temperature >= 0.0 AND daily_avg_temperature <= 200.0));

ALTER TABLE gold.equipment_health ADD CONSTRAINT chk_health_vibration_range
    CHECK (daily_avg_vibration IS NULL OR (daily_avg_vibration >= 0.0 AND daily_avg_vibration <= 20.0));

ALTER TABLE gold.equipment_health ADD CONSTRAINT chk_health_abnormal_non_negative
    CHECK (abnormal_count >= 0);

ALTER TABLE gold.equipment_health ADD CONSTRAINT chk_health_measurement_positive
    CHECK (measurement_count > 0);

ALTER TABLE gold.equipment_health ADD CONSTRAINT chk_health_status_values
    CHECK (status_summary IN ('正常', '警告', '異常', '停止'));
