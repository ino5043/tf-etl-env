-- =============================================================================
-- Silver Layer DDL: production_facts, equipment_master
-- =============================================================================
-- Platform:    Azure Fabric Lakehouse (Delta Lake / Spark SQL)
-- Purpose:     型変換・クレンジング済みの分析可能データ
-- Partition:   production_date (生産日) — production_facts のみ
-- Idempotency: DELETE + INSERT by production_date
-- =============================================================================

-- スキーマ作成（存在しない場合）
CREATE SCHEMA IF NOT EXISTS silver;

-- -----------------------------------------------------------------------------
-- production_facts: 生産実績ファクトテーブル
-- -----------------------------------------------------------------------------
-- Bronze の生データを型変換・クレンジングし、分析可能な構造にしたテーブル。
-- 計算列（achievement_rate, defect_rate, good_quantity）を事前算出する。
-- パーティション単位の DELETE + INSERT で冪等な更新を実現する。
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS silver.production_facts (
    -- サロゲートキー
    production_fact_id  BIGINT GENERATED ALWAYS AS IDENTITY
                                COMMENT 'サロゲートキー（自動採番）',

    -- ビジネスキー・ディメンション
    production_date     DATE        NOT NULL    COMMENT '生産日',
    line_id             STRING      NOT NULL    COMMENT '生産ライン ID（LINE-001〜LINE-005）',
    product_code        STRING      NOT NULL    COMMENT '製品コード（PRD-A001〜PRD-A020）',
    equipment_id        STRING      NOT NULL    COMMENT '設備 ID（EQ-001〜EQ-010）',
    shift               STRING      NOT NULL    COMMENT 'シフト（日勤/夜勤）',
    operator_id         STRING      NOT NULL    COMMENT '作業者 ID（OP-001〜OP-050）',

    -- 数量メジャー
    planned_quantity    INT         NOT NULL    COMMENT '計画数量',
    actual_quantity     INT         NOT NULL    COMMENT '実績数量',
    defect_quantity     INT         NOT NULL    COMMENT '不良数量',
    good_quantity       INT         NOT NULL    COMMENT '良品数量（actual_quantity - defect_quantity）',

    -- 計算メジャー
    achievement_rate    DOUBLE      NOT NULL    COMMENT '達成率（actual_quantity / planned_quantity）',
    defect_rate         DOUBLE      NOT NULL    COMMENT '不良率（defect_quantity / actual_quantity）',

    -- センサーデータ
    temperature         DOUBLE                  COMMENT '温度（℃）。センサー欠損時 NULL',
    vibration           DOUBLE                  COMMENT '振動（mm/s）。センサー欠損時 NULL',

    -- 設備状態
    status              STRING      NOT NULL    COMMENT '設備状態（正常/警告/異常/停止）',

    -- タイムスタンプ
    source_created_at   TIMESTAMP   NOT NULL    COMMENT 'ソースレコード作成日時',

    -- メタデータ
    _processed_at       TIMESTAMP   NOT NULL    COMMENT 'Silver 変換処理日時',
    _source_file        STRING      NOT NULL    COMMENT '元ファイル名（リネージ）',
    _batch_id           STRING      NOT NULL    COMMENT 'バッチ実行 ID'
)
USING DELTA
PARTITIONED BY (production_date)
COMMENT '生産実績ファクトテーブル（Silver レイヤー）。型変換・クレンジング済み。'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true',
    'delta.logRetentionDuration'       = 'interval 30 days',
    'delta.deletedFileRetentionDuration' = 'interval 7 days'
);

-- -----------------------------------------------------------------------------
-- production_facts: CHECK 制約
-- -----------------------------------------------------------------------------

ALTER TABLE silver.production_facts ADD CONSTRAINT chk_planned_qty_positive
    CHECK (planned_quantity > 0);

ALTER TABLE silver.production_facts ADD CONSTRAINT chk_actual_qty_non_negative
    CHECK (actual_quantity >= 0);

ALTER TABLE silver.production_facts ADD CONSTRAINT chk_defect_qty_non_negative
    CHECK (defect_quantity >= 0);

ALTER TABLE silver.production_facts ADD CONSTRAINT chk_defect_lte_actual
    CHECK (defect_quantity <= actual_quantity);

ALTER TABLE silver.production_facts ADD CONSTRAINT chk_good_qty_calculation
    CHECK (good_quantity = actual_quantity - defect_quantity);

ALTER TABLE silver.production_facts ADD CONSTRAINT chk_shift_values
    CHECK (shift IN ('日勤', '夜勤'));

ALTER TABLE silver.production_facts ADD CONSTRAINT chk_status_values
    CHECK (status IN ('正常', '警告', '異常', '停止'));

ALTER TABLE silver.production_facts ADD CONSTRAINT chk_temperature_range
    CHECK (temperature IS NULL OR (temperature >= 15.0 AND temperature <= 100.0));

ALTER TABLE silver.production_facts ADD CONSTRAINT chk_vibration_range
    CHECK (vibration IS NULL OR (vibration >= 0.0 AND vibration <= 10.0));

-- -----------------------------------------------------------------------------
-- equipment_master: 設備マスタ（SCD Type 2）
-- -----------------------------------------------------------------------------
-- 設備の属性変更履歴を管理する Slowly Changing Dimension Type 2 テーブル。
-- production_facts から equipment_id + line_id の組み合わせを抽出して構築する。
-- valid_from / valid_to / is_current で有効期間を管理する。
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS silver.equipment_master (
    -- サロゲートキー
    equipment_sk        BIGINT GENERATED ALWAYS AS IDENTITY
                                COMMENT 'サロゲートキー（自動採番）',

    -- ナチュラルキー
    equipment_id        STRING      NOT NULL    COMMENT '設備ナチュラルキー（EQ-001〜EQ-010）',

    -- 設備属性
    line_id             STRING      NOT NULL    COMMENT '設置ライン ID',
    latest_status       STRING      NOT NULL    COMMENT '最新の設備状態（正常/警告/異常/停止）',

    -- SCD Type 2 管理列
    valid_from          DATE        NOT NULL    COMMENT '有効開始日',
    valid_to            DATE                    COMMENT '有効終了日（現行レコードは NULL）',
    is_current          BOOLEAN     NOT NULL    COMMENT '現行フラグ（true = 最新レコード）',

    -- メタデータ
    _updated_at         TIMESTAMP   NOT NULL    COMMENT 'レコード更新日時'
)
USING DELTA
COMMENT '設備マスタ（Silver レイヤー）。SCD Type 2 で設備属性の変更履歴を管理。'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true',
    'delta.logRetentionDuration'       = 'interval 30 days',
    'delta.deletedFileRetentionDuration' = 'interval 7 days'
);

-- -----------------------------------------------------------------------------
-- equipment_master: CHECK 制約
-- -----------------------------------------------------------------------------

ALTER TABLE silver.equipment_master ADD CONSTRAINT chk_equip_status_values
    CHECK (latest_status IN ('正常', '警告', '異常', '停止'));

ALTER TABLE silver.equipment_master ADD CONSTRAINT chk_equip_valid_period
    CHECK (valid_to IS NULL OR valid_to >= valid_from);
