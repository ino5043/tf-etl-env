# スキーマ設計書 — 製造業 IoT デモ

| 項目 | 値 |
|------|-----|
| プロジェクト | 製造業 IoT デモ（工場生産実績パイプライン） |
| プラットフォーム | Azure Fabric Lakehouse（Delta Lake） |
| アーキテクチャ | Medallion Architecture（Bronze → Silver → Gold） |
| 作成日 | 2026-03-15 |
| 作成者 | schema-designer agent |

---

## 1. 設計概要

### 1.1 レイヤー別テーブル一覧

| レイヤー | テーブル名 | 目的 | パーティション |
|---------|-----------|------|---------------|
| Bronze | `raw_production` | CSV 生データの忠実な保存 | `_ingested_date` |
| Silver | `production_facts` | 型変換・クレンジング済み実績データ | `production_date` |
| Silver | `equipment_master` | 設備マスタ（SCD Type 2） | なし |
| Gold | `daily_production_kpi` | 日次 KPI 集計（ライン別） | `production_date` |
| Gold | `equipment_health` | 設備健全性（7日間移動平均） | `production_date` |

### 1.2 設計方針

| 方針 | 決定 | 理由 |
|------|------|------|
| Bronze の型 | 全列 STRING | CSV 取り込み時の型変換エラーを防止。生データの忠実な保存を優先 |
| Silver の型変換 | 厳密な型付け | ビジネスロジック適用前にデータ品質を担保 |
| Gold のモデリング | Star Schema | Power BI との親和性が高く、集計クエリが高速 |
| パーティション | `production_date`（日次） | 日次バッチの処理単位と一致。冪等な上書きが容易 |
| SCD 方式 | Type 2（設備マスタ） | 設備の設置場所・状態の変更履歴を保持 |
| 文字コード | UTF-8 | Fabric 標準。CSV の BOM は取り込み時に除去 |

---

## 2. Bronze レイヤー

### 2.1 raw_production

CSV ファイルから取り込んだ生データをそのまま格納する。全列を STRING 型とし、型変換エラーによるデータ欠損を防止する。

**設計判断:**

- 全列 STRING: ソースデータの忠実な保存。型変換は Silver で実施
- メタデータ列の追加: データリネージとバッチ管理のため `_ingested_at`, `_source_file`, `_batch_id` を付与
- パーティション: `_ingested_date`（取り込み日）で管理。日次バッチの冪等な再実行を担保

| # | 列名 | 型 | 説明 |
|---|------|-----|------|
| 1 | `production_date` | STRING | 生産日（ソース値そのまま） |
| 2 | `line_id` | STRING | 生産ライン ID |
| 3 | `product_code` | STRING | 製品コード |
| 4 | `planned_quantity` | STRING | 計画数量 |
| 5 | `actual_quantity` | STRING | 実績数量 |
| 6 | `defect_quantity` | STRING | 不良数量 |
| 7 | `equipment_id` | STRING | 設備 ID |
| 8 | `shift` | STRING | シフト（日勤/夜勤） |
| 9 | `operator_id` | STRING | 作業者 ID |
| 10 | `temperature` | STRING | 温度（℃） |
| 11 | `vibration` | STRING | 振動（mm/s） |
| 12 | `status` | STRING | 設備状態 |
| 13 | `created_at` | STRING | レコード作成日時（ソース値そのまま） |
| 14 | `_ingested_at` | TIMESTAMP | 取り込み日時（パイプライン付与） |
| 15 | `_source_file` | STRING | 取り込み元ファイル名 |
| 16 | `_batch_id` | STRING | バッチ実行 ID |

---

## 3. Silver レイヤー

### 3.1 production_facts

Bronze の生データを型変換・クレンジングし、分析可能な構造にしたファクトテーブル。

**設計判断:**

- 厳密な型付け: DATE, INT, FLOAT 等に変換。型変換失敗レコードはエラーテーブルに分離
- 計算列の追加: `achievement_rate`（達成率）、`defect_rate`（不良率）を事前計算し、集計クエリの負荷を軽減
- パーティション: `production_date`（日次）。Silver の冪等な上書き（DELETE + INSERT）に最適

| # | 列名 | 型 | NULL許可 | 説明 |
|---|------|-----|---------|------|
| 1 | `production_fact_id` | BIGINT | NO | サロゲートキー（自動採番） |
| 2 | `production_date` | DATE | NO | 生産日 |
| 3 | `line_id` | STRING | NO | 生産ライン ID（LINE-001〜LINE-005） |
| 4 | `product_code` | STRING | NO | 製品コード（PRD-A001〜PRD-A020） |
| 5 | `planned_quantity` | INT | NO | 計画数量 |
| 6 | `actual_quantity` | INT | NO | 実績数量 |
| 7 | `defect_quantity` | INT | NO | 不良数量 |
| 8 | `good_quantity` | INT | NO | 良品数量（actual_quantity - defect_quantity） |
| 9 | `achievement_rate` | DOUBLE | NO | 達成率（actual_quantity / planned_quantity） |
| 10 | `defect_rate` | DOUBLE | NO | 不良率（defect_quantity / actual_quantity） |
| 11 | `equipment_id` | STRING | NO | 設備 ID（EQ-001〜EQ-010） |
| 12 | `shift` | STRING | NO | シフト（日勤/夜勤） |
| 13 | `operator_id` | STRING | NO | 作業者 ID（OP-001〜OP-050） |
| 14 | `temperature` | DOUBLE | YES | 温度（℃）。センサー欠損時 NULL |
| 15 | `vibration` | DOUBLE | YES | 振動（mm/s）。センサー欠損時 NULL |
| 16 | `status` | STRING | NO | 設備状態（正常/警告/異常/停止） |
| 17 | `source_created_at` | TIMESTAMP | NO | ソースレコード作成日時 |
| 18 | `_processed_at` | TIMESTAMP | NO | Silver 変換処理日時 |
| 19 | `_source_file` | STRING | NO | 元ファイル名（リネージ） |
| 20 | `_batch_id` | STRING | NO | バッチ実行 ID |

**バリデーションルール:**

| 列 | ルール | 違反時の処理 |
|----|--------|-------------|
| `production_date` | DATE 型に変換可能 | エラーテーブルへ分離 |
| `planned_quantity` | 正の整数 | エラーテーブルへ分離 |
| `actual_quantity` | 0 以上の整数 | エラーテーブルへ分離 |
| `defect_quantity` | 0 以上かつ actual_quantity 以下 | エラーテーブルへ分離 |
| `temperature` | 15.0〜100.0（許容範囲） | NULL に変換 + 警告ログ |
| `vibration` | 0.0〜10.0（許容範囲） | NULL に変換 + 警告ログ |
| `status` | 正常/警告/異常/停止 のいずれか | エラーテーブルへ分離 |
| `shift` | 日勤/夜勤 のいずれか | エラーテーブルへ分離 |

### 3.2 equipment_master

設備マスタ。SCD Type 2 で設備の属性変更履歴を管理する。

**設計判断:**

- SCD Type 2: 設備の設置ライン変更や状態変更の履歴を保持。予兆保全の分析に必要
- ソース: production_facts から equipment_id + line_id の組み合わせを抽出して構築
- `is_current` フラグ: 最新レコードの検索を高速化

| # | 列名 | 型 | NULL許可 | 説明 |
|---|------|-----|---------|------|
| 1 | `equipment_sk` | BIGINT | NO | サロゲートキー（自動採番） |
| 2 | `equipment_id` | STRING | NO | 設備ナチュラルキー（EQ-001〜EQ-010） |
| 3 | `line_id` | STRING | NO | 設置ライン ID |
| 4 | `latest_status` | STRING | NO | 最新の設備状態 |
| 5 | `valid_from` | DATE | NO | 有効開始日 |
| 6 | `valid_to` | DATE | YES | 有効終了日（現行は NULL） |
| 7 | `is_current` | BOOLEAN | NO | 現行フラグ（true = 最新） |
| 8 | `_updated_at` | TIMESTAMP | NO | レコード更新日時 |

---

## 4. Gold レイヤー

### 4.1 daily_production_kpi

日次 × ライン別の生産 KPI 集計テーブル。工場長ダッシュボードの主要データソース。

**設計判断:**

- 集計粒度: 日次 × ライン。工場長の朝会での確認単位に合致
- Star Schema: ファクトテーブルとして設計。ディメンションは `line_id`, `production_date`
- 事前集計: SUM, AVG, MAX, MIN を事前計算し、Power BI のクエリ負荷を軽減

| # | 列名 | 型 | NULL許可 | 説明 |
|---|------|-----|---------|------|
| 1 | `production_date` | DATE | NO | 生産日 |
| 2 | `line_id` | STRING | NO | 生産ライン ID |
| 3 | `total_planned_quantity` | BIGINT | NO | 計画数量合計 |
| 4 | `total_actual_quantity` | BIGINT | NO | 実績数量合計 |
| 5 | `total_defect_quantity` | BIGINT | NO | 不良数量合計 |
| 6 | `total_good_quantity` | BIGINT | NO | 良品数量合計 |
| 7 | `achievement_rate` | DOUBLE | NO | 達成率（実績合計 / 計画合計） |
| 8 | `defect_rate` | DOUBLE | NO | 不良率（不良合計 / 実績合計） |
| 9 | `yield_rate` | DOUBLE | NO | 歩留まり率（良品合計 / 実績合計） |
| 10 | `record_count` | INT | NO | 集計元レコード数 |
| 11 | `unique_products` | INT | NO | 製造品目数 |
| 12 | `unique_operators` | INT | NO | 作業者数 |
| 13 | `day_shift_quantity` | BIGINT | NO | 日勤の実績数量 |
| 14 | `night_shift_quantity` | BIGINT | NO | 夜勤の実績数量 |
| 15 | `_aggregated_at` | TIMESTAMP | NO | 集計実行日時 |

**主キー:** `(production_date, line_id)`

### 4.2 equipment_health

設備健全性テーブル。7日間の移動平均で温度・振動のトレンドを追跡し、予兆保全に活用する。

**設計判断:**

- 移動平均: 7日間。短期的なノイズを除去しつつトレンドを可視化
- アラートフラグ: 閾値超過を事前判定し、Power BI でのアラート表示を高速化
- 集計粒度: 日次 × 設備。設備保全チームの分析単位に合致

| # | 列名 | 型 | NULL許可 | 説明 |
|---|------|-----|---------|------|
| 1 | `production_date` | DATE | NO | 対象日 |
| 2 | `equipment_id` | STRING | NO | 設備 ID |
| 3 | `line_id` | STRING | NO | 設置ライン ID |
| 4 | `daily_avg_temperature` | DOUBLE | YES | 当日平均温度（℃） |
| 5 | `daily_max_temperature` | DOUBLE | YES | 当日最高温度（℃） |
| 6 | `daily_avg_vibration` | DOUBLE | YES | 当日平均振動（mm/s） |
| 7 | `daily_max_vibration` | DOUBLE | YES | 当日最大振動（mm/s） |
| 8 | `rolling_7d_avg_temperature` | DOUBLE | YES | 7日間移動平均温度（℃） |
| 9 | `rolling_7d_avg_vibration` | DOUBLE | YES | 7日間移動平均振動（mm/s） |
| 10 | `temperature_alert` | BOOLEAN | NO | 温度アラート（7日平均 > 65℃） |
| 11 | `vibration_alert` | BOOLEAN | NO | 振動アラート（7日平均 > 3.5 mm/s） |
| 12 | `status_summary` | STRING | NO | 当日の状態サマリ（最も深刻な状態） |
| 13 | `abnormal_count` | INT | NO | 当日の異常/停止発生回数 |
| 14 | `measurement_count` | INT | NO | 当日の計測レコード数 |
| 15 | `_aggregated_at` | TIMESTAMP | NO | 集計実行日時 |

**主キー:** `(production_date, equipment_id)`

**アラート閾値:**

| メトリクス | Warning | Critical | 根拠 |
|-----------|---------|----------|------|
| 7日平均温度 | > 60℃ | > 65℃ | 設備仕様の上限温度 80℃ の 75%/81% |
| 7日平均振動 | > 3.0 mm/s | > 3.5 mm/s | ISO 10816 振動基準 Zone C/D 相当 |

---

## 5. パーティション戦略

| テーブル | パーティションキー | 粒度 | 理由 |
|---------|-------------------|------|------|
| `bronze.raw_production` | `_ingested_date` | 日次 | バッチ取り込み単位。再実行時のパーティション上書き |
| `silver.production_facts` | `production_date` | 日次 | ビジネスキーでのパーティション。DELETE + INSERT で冪等 |
| `silver.equipment_master` | なし | - | 少量データ（最大数十行）。パーティション不要 |
| `gold.daily_production_kpi` | `production_date` | 日次 | Power BI の日付フィルターと一致 |
| `gold.equipment_health` | `production_date` | 日次 | 移動平均の再計算範囲と一致 |

---

## 6. データフロー

```
CSV ファイル
  │
  ▼
Bronze: raw_production（全 STRING + メタデータ）
  │
  ├──→ Silver: production_facts（型変換・クレンジング・計算列追加）
  │       │
  │       ├──→ Gold: daily_production_kpi（日次 × ライン集計）
  │       │
  │       └──→ Gold: equipment_health（日次 × 設備 + 7日移動平均）
  │
  └──→ Silver: equipment_master（SCD Type 2 で設備属性を管理）
```

---

## 7. ER 図

ER 図は `diagrams/er-diagram.mmd` に Mermaid 形式で格納している。

---

## 8. 関連ドキュメント

| ドキュメント | パス | 内容 |
|-------------|------|------|
| 要件定義 | `../01_requirements/requirements.yaml` | データソース・SLA・ビジネスコンテキスト |
| DDL（Bronze） | `ddl/bronze.sql` | Bronze テーブルの CREATE 文 |
| DDL（Silver） | `ddl/silver.sql` | Silver テーブルの CREATE 文 |
| DDL（Gold） | `ddl/gold.sql` | Gold テーブルの CREATE 文 |
| ER 図 | `diagrams/er-diagram.mmd` | テーブル間リレーション |
