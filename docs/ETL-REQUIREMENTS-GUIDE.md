# ETL 開発 要件ガイド

tf-etl-env を使って ETL パイプラインを開発する際の工程と、各工程でユーザーが整理すべき情報をまとめる。

---

## 全体工程フロー

```
Phase 1: 要件定義        ─→ 何を、どこから、どこへ、いつまでに
Phase 2: アーキテクチャ設計 ─→ どうやって運ぶか
Phase 3: スキーマ設計     ─→ どう格納するか
Phase 4: 品質設計        ─→ どう守るか
Phase 5: 実装           ─→ つくる
Phase 6: テスト          ─→ 確かめる
Phase 7: 運用設計        ─→ 回し続ける
```

各フェーズの出力が次フェーズの入力になる。Phase 1〜4 が設計、Phase 5〜7 が実行。

---

## 情報補完のルール

ユーザーが空欄や曖昧な情報を渡した場合、エージェントは以下のルールに従う。

### 1. 推論と事実を明確に区別する

エージェントが不足情報を補完する際、**推論した部分**と**ユーザーから提供された事実**を必ず区別して表示する。

```markdown
## パイプライン方式の提案

| 項目 | 値 | 根拠 |
|------|-----|------|
| 方式 | 日次バッチ ETL | ⚡ 推論: 日次 CSV + AM8:00 期限から判断 |
| レイヤー | Bronze → Silver → Gold | ⚡ 推論: BI 利用のため3層が適切と判断 |
| 形式 | Delta Table | ✅ 確定: ユーザー指定 |
| スケジュール | 毎日 AM 5:00 | ⚡ 推論: 期限 AM 8:00 に対し3時間のバッファ |
```

**表記ルール:**

- ✅ **確定**: ユーザーが明示的に提供した情報
- ⚡ **推論**: エージェントが文脈から推測した情報（根拠を必ず併記）
- ❓ **未定**: 情報不足で判断できない項目（質問として提示）

### 2. 推論部分は必ず確認を取る

推論した項目は一括で確認を求める。ユーザーが修正しやすいように選択肢を提示する。

```markdown
### 確認事項

以下は推論で補完しました。認識が合っているか確認してください。

1. **パイプライン方式を日次バッチにしました**
   → 日次 CSV + AM 8:00 期限から判断しました。
   → 他の選択肢: 時間単位バッチ / CDC / ストリーミング
   → [ OK / 変更 ]

2. **スケジュールを AM 5:00 にしました**
   → AM 8:00 期限に対し 3 時間のバッファを確保しました。
   → もっと早める/遅くする場合は教えてください。
   → [ OK / 変更 ]

3. **Medallion 3層構成にしました**
   → BI ダッシュボード利用のため Gold 層まで必要と判断しました。
   → Silver 止めで十分な場合はお知らせください。
   → [ OK / 変更 ]
```

### 3. 質問は積極的に行う

推論でカバーできない項目は、遠慮なく質問する。質問のフォーマット:

```markdown
### 質問（Phase N を進めるために必要です）

❓ **Q1: ソースデータの更新パターンは？**
   背景: 全件洗替か差分かで、パイプライン方式が大きく変わります。
   選択肢: 全件洗替 / 差分（updated_at 基準）/ CDC / わからない

❓ **Q2: 既存テーブルはありますか？**
   背景: 移行が必要な場合、スキーマ進化戦略を先に決める必要があります。
   選択肢: ある（DDL提供可）/ ある（DDL不明）/ 新規作成 / わからない
```

### 4. 「わからない」は正当な回答

ユーザーが「わからない」と回答した場合:

- エージェントが**安全側のデフォルト値**を提案する
- デフォルト値にも ⚡ 推論マークを付ける
- 後フェーズで情報が確定したら更新する

---

## Phase 1: 要件定義

**目的**: パイプラインの全体像を把握する。ここが曖昧だと後工程すべてがブレる。

### 1.1 データソース定義

パイプラインごとに 1 つ記入する。複数ソースがある場合はソースごとに作成。

```yaml
# === データソース定義 ===
source:
  name: ""                    # ソース名（例: 生産実績DB、受注CSV）
  system: ""                  # システム名（例: SAP, Salesforce, 社内PostgreSQL）
  format: ""                  # CSV | JSON | Parquet | RDB Table | API
  connection: ""              # JDBC | REST API | ファイル転送 | CDC
  volume:
    daily_records: ""         # 日次レコード数（例: 10,000件）
    daily_size: ""            # 日次データ量（例: 500MB）
    growth_rate: ""           # 年間増加見込み（例: 月+5%）
  update_pattern: ""          # 全件洗替 | 差分 | CDC（変更データキャプチャ）
  update_frequency: ""        # リアルタイム | 時間単位 | 日次 | 週次
  schema_stability: ""        # 安定 | 年数回変更 | 頻繁に変更
  data_owner: ""              # データオーナー（部門・担当者）
  notes: ""                   # 特記事項（文字コード、改行コード等）
```

### 1.2 データターゲット定義

```yaml
# === データターゲット定義 ===
target:
  name: ""                    # ターゲット名（例: 生産実績ファクトテーブル）
  system: ""                  # 格納先（例: Delta Lake, SQL Server, BigQuery）
  format: ""                  # Delta Table | Parquet | SQL Table
  usage: ""                   # BI | ML | API | レポート | 他パイプラインの入力
  consumers:                  # 利用者一覧
    - name: ""                # 利用者/システム名
      access_pattern: ""      # OLAP | OLTP | バッチ読取
      priority: ""            # 高 | 中 | 低
  retention: ""               # データ保持期間（例: 3年、無期限）
```

### 1.3 SLA・制約条件

```yaml
# === SLA・制約条件 ===
sla:
  freshness_deadline: ""      # データ準備完了期限（例: 毎日 AM 8:00 JST）
  max_delay: ""               # 許容最大遅延（例: 2時間）
  availability: ""            # 可用性目標（例: 99.5%）
  error_rate_threshold: ""    # 許容エラー率（例: < 1%）

constraints:
  budget: ""                  # 月額予算上限（例: $500）
  platform: ""                # 利用プラットフォーム（例: Azure Fabric F2）
  compliance: ""              # 規制要件（例: 個人情報保護法、GDPR）
  data_classification: ""     # 機密 | 社外秘 | 一般
  team_skills: ""             # チームスキル（例: Python○, Spark△, SQL○）
```

### 1.4 ビジネスコンテキスト

```yaml
# === ビジネスコンテキスト ===
business:
  purpose: ""                 # なぜこのパイプラインが必要か（1-2文）
  stakeholders:               # ステークホルダー
    - role: ""                # 役割（例: 工場長、データアナリスト）
      expectation: ""         # 期待すること
  success_criteria: ""        # 成功基準（例: ダッシュボードで前日実績が見える）
  known_issues: ""            # 現状の課題（例: 手作業で3時間かかっている）
```

---

## Phase 2: アーキテクチャ設計

**目的**: パイプラインの処理方式とレイヤー構成を決める。
**使うもの**: `pipeline-architect` エージェント

### 2.1 パイプライン方式選択

Phase 1 の情報を基に判断する。以下は判断材料の補足。

```yaml
# === パイプライン方式の判断材料 ===
pipeline_decision:
  # 以下の質問に答える（該当するものを選択）
  realtime_needed: Yes | No   # リアルタイム処理が必要か？
  target_has_compute: Yes | No # ターゲット側に計算資源があるか？（ELT向き）
  history_tracking: Yes | No  # 履歴変更を追跡する必要があるか？（CDC向き）

  # 方式（pipeline-architect が提案、ユーザーが承認）
  # ETL | ELT | CDC | Streaming | Micro-batch
  selected_pattern: ""
  reason: ""                  # 選択理由
```

### 2.2 Medallion レイヤー設計

```yaml
# === Medallion レイヤー設計 ===
medallion:
  bronze:
    description: ""           # このパイプラインの Bronze の役割
    storage: ""               # 格納先（例: abfss://bronze@storage/）
    format: ""                # Parquet | Delta | JSON（そのまま保存）
    retention: ""             # 保持期間
  silver:
    description: ""           # クレンジング・正規化の内容
    transformations:          # 主な変換内容を列挙
      - ""
    quality_gate: ""          # Silver 昇格条件（例: NULL率 < 5%）
  gold:
    description: ""           # ビジネス用途向け加工の内容
    aggregations:             # 集約・結合の内容
      - ""
    consumers: []             # 利用先（Phase 1.2 から転記）
```

### 2.3 パイプライン依存関係

```yaml
# === 依存関係 ===
dependencies:
  upstream:                   # このパイプラインの前に完了すべきもの
    - pipeline: ""
      condition: ""           # 完了条件
  downstream:                 # このパイプラインの後に実行されるもの
    - pipeline: ""
      trigger: ""             # トリガー条件
  external:                   # 外部システム依存
    - system: ""
      dependency_type: ""     # データ取得 | API呼出 | 認証
```

---

## Phase 3: スキーマ設計

**目的**: 各レイヤーのテーブル構造を決める。
**使うもの**: `schema-designer` エージェント、`/schema-review` コマンド

### 3.1 テーブル定義

レイヤーごと・テーブルごとに記入する。

```yaml
# === テーブル定義 ===
table:
  layer: ""                   # Bronze | Silver | Gold
  name: ""                    # テーブル名（例: gold.fact_production）
  description: ""             # ビジネス上の意味
  grain: ""                   # 粒度（例: 1行 = 1製造ロット）

  columns:
    - name: ""
      type: ""                # STRING | INT | DECIMAL(10,2) | TIMESTAMP | ...
      nullable: Yes | No
      description: ""         # ビジネス上の意味
      pii: Yes | No           # 個人情報を含むか
      example: ""             # 値の例

  keys:
    primary: []               # 主キーカラム
    foreign:                  # 外部キー
      - column: ""
        references: ""        # 参照先テーブル.カラム

  # ===スキーマ設計の判断材料 ===
  design_decisions:
    normalization: ""         # 正規化 | 非正規化 | スタースキーマ
    scd_type: ""              # None | Type1（上書き）| Type2（履歴保持）
    partition_key: ""         # パーティションキー（例: process_date）
    partition_granularity: "" # daily | monthly | yearly
    sort_key: ""              # ソートキー（クエリ高速化用）
```

### 3.2 既存スキーマ（あれば）

既存テーブルからの移行の場合、現行スキーマを記載する。

```yaml
# === 既存スキーマ ===
existing_schema:
  provided: Yes | No
  source: ""                  # DDL ファイルパス or テーブル名
  known_issues:               # 現行スキーマの課題
    - ""
```

---

## Phase 4: 品質設計

**目的**: データ品質ルールと監視方法を決める。
**使うもの**: `data-quality-reviewer` エージェント、`/dq-check` コマンド

### 4.1 品質ルール定義

テーブルごとに記入する。

```yaml
# === 品質ルール定義 ===
quality_rules:
  table: ""                   # 対象テーブル名

  # --- 完全性（Completeness）---
  completeness:
    mandatory_columns: []     # NULL不可カラム一覧
    max_null_rate:            # カラム別 NULL 許容率
      column_name: "5%"       # 例: 5% まで許容

  # --- 正確性（Accuracy）---
  accuracy:
    range_checks:             # 値の範囲チェック
      - column: ""
        min: ""
        max: ""
    enum_checks:              # 許容値リスト
      - column: ""
        values: []
    regex_checks:             # フォーマットチェック
      - column: ""
        pattern: ""           # 例: "^\d{4}-\d{2}-\d{2}$"

  # --- 一貫性（Consistency）---
  consistency:
    cross_table_rules:        # テーブル間整合性
      - rule: ""              # 例: "orders.customer_id は customers に存在する"
    calculation_rules:        # 計算整合性
      - rule: ""              # 例: "合計 = 単価 × 数量"

  # --- 適時性（Timeliness）---
  timeliness:
    freshness_column: ""      # 鮮度判定カラム（例: updated_at）
    max_age_hours: ""         # 最大許容遅延（時間）

  # --- 一意性（Uniqueness）---
  uniqueness:
    unique_keys: []           # ユニーク制約カラム
    duplicate_tolerance: ""   # 重複許容率（例: 0%）

  # --- 有効性（Validity）---
  validity:
    format_rules:             # 型・フォーマット
      - column: ""
        expected_type: ""
    business_rules:           # ビジネスルール
      - rule: ""              # 例: "出荷日 >= 受注日"
```

### 4.2 品質閾値と対応

```yaml
# === 品質閾値 ===
quality_thresholds:
  # 品質スコアに対するアクション
  warning_threshold: ""       # 例: 品質スコア < 95% → 警告
  critical_threshold: ""      # 例: 品質スコア < 90% → パイプライン停止

  on_violation: ""            # fail（停止）| warn（警告して続行）| quarantine（隔離して続行）
  error_table: ""             # エラーデータ格納先（例: bronze.dead_letter_orders）

  notification:
    channel: ""               # Slack | Email | Teams
    recipients: []            # 通知先
```

---

## Phase 5: 実装

**目的**: コードを書く。
**使うもの**: `pipeline-scaffold` スキル、`data-contract` スキル、`test-data-generator` スキル

### 5.1 実装指示シート

```yaml
# === 実装指示 ===
implementation:
  orchestrator: ""            # Airflow | ADF | Fabric Pipeline
  language: ""                # PySpark | SQL | Python
  schedule: ""                # cron式（例: "0 5 * * *" = 毎日 AM5:00）

  # 変換ロジックの要件（自然言語で記述）
  transformations:
    - step: 1
      name: ""                # ステップ名（例: "受注データ抽出"）
      input: ""               # 入力（例: source.orders テーブル）
      output: ""              # 出力（例: bronze.raw_orders）
      logic: ""               # 処理内容（例: "WHERE order_date = {{target_date}}"）
    - step: 2
      name: ""
      input: ""
      output: ""
      logic: ""

  error_handling:
    strategy: ""              # fail-fast | collect-and-continue
    retry_count: ""           # リトライ回数（例: 3）
    retry_interval: ""        # リトライ間隔（例: exponential, base=2s）
```

### 5.2 データ契約（任意）

プロデューサー/コンシューマー間の合意が必要な場合に作成。

```yaml
# === データ契約 ===
data_contract:
  needed: Yes | No
  producer: ""                # 提供側チーム
  consumers: []               # 利用側チーム
  breaking_change_notice: ""  # 破壊的変更の事前通知期間（例: 2週間）
```

---

## Phase 6: テスト

**目的**: パイプラインの正しさを検証する。
**使うもの**: `etl-tdd-guide` エージェント、`/etl-tdd` コマンド、`test-data-generator` スキル

### 6.1 テスト要件

```yaml
# === テスト要件 ===
testing:
  coverage_target: ""         # 目標カバレッジ（例: 80%）

  # テストレベル別の要件
  unit_tests:
    scope: ""                 # 例: "各変換関数の入出力検証"
    critical_paths:           # 必ずテストすべきロジック
      - ""

  integration_tests:
    scope: ""                 # 例: "Bronze→Silver パイプライン全体"
    external_mocks:           # モック対象の外部システム
      - ""

  # テストデータの要件
  test_data:
    normal_cases: ""          # 正常ケースの説明
    edge_cases:               # エッジケース
      - ""                    # 例: "NULLだらけのレコード"
      - ""                    # 例: "日付が未来"
      - ""                    # 例: "数値がマイナス"
    error_cases:              # 異常ケース
      - ""                    # 例: "スキーマ不一致のレコード"
    volume: ""                # テストデータ件数（例: 1,000件）
    locale: ""                # Faker ロケール（例: ja_JP）
```

---

## Phase 7: 運用設計

**目的**: 本番稼働後の監視・障害対応・変更管理を設計する。
**使うもの**: `sla-validator` エージェント、`incident-runbook` スキル、`/lineage` コマンド

### 7.1 監視設計

```yaml
# === 監視設計 ===
monitoring:
  dashboard_needed: Yes | No

  # アラート設定
  alerts:
    - metric: ""              # 例: "エラー率"
      warning: ""             # 例: "> 5%"
      critical: ""            # 例: "> 10%"
      action: ""              # 例: "Slack #data-alert に通知"
    - metric: ""              # 例: "処理時間"
      warning: ""             # 例: "> 平均の2倍"
      critical: ""            # 例: "> 平均の5倍"
      action: ""

  # ログ要件
  logging:
    level: ""                 # INFO | DEBUG
    structured: Yes           # 構造化ログ（JSON）
    retention: ""             # ログ保持期間
```

### 7.2 障害対応

```yaml
# === 障害対応 ===
incident_response:
  runbook_needed: Yes | No

  # 想定障害シナリオ
  scenarios:
    - failure: ""             # 例: "ソースDB接続失敗"
      impact: ""              # 例: "Bronze更新停止、Goldデータ陳腐化"
      recovery: ""            # 例: "接続復旧後、再実行（冪等性あり）"
    - failure: ""             # 例: "データ品質閾値超過"
      impact: ""
      recovery: ""

  # 連絡体制
  escalation:
    - level: 1
      who: ""                 # 例: "データエンジニアチーム"
      when: ""                # 例: "Warning アラート発生時"
    - level: 2
      who: ""
      when: ""                # 例: "Critical アラート or 1時間未解決"
```

### 7.3 変更管理

```yaml
# === 変更管理 ===
change_management:
  schema_change_process: ""   # 例: "ADR作成 → レビュー → Migration Plan → 実行"
  rollback_strategy: ""       # 例: "Blue-Green デプロイ + 自動ロールバック"

  # 将来の変更予定（わかっていれば）
  planned_changes:
    - timing: ""              # 例: "3ヶ月後"
      change: ""              # 例: "リアルタイム取り込みへの移行"
```

---

## 工程とコンポーネントの対応表

| Phase | ユーザーが準備する情報 | 使う Agent/Skill/Command | 成果物 |
|-------|---------------------|------------------------|--------|
| **1. 要件定義** | ソース、ターゲット、SLA、ビジネス背景 | — (ヒアリング) | 要件定義書 |
| **2. アーキテクチャ** | 方式選択の判断材料、依存関係 | `pipeline-architect` | アーキテクチャ図、ADR |
| **3. スキーマ** | テーブル定義、既存スキーマ | `schema-designer`, `/schema-review` | DDL、ER図 |
| **4. 品質** | 品質ルール、閾値 | `data-quality-reviewer`, `/dq-check` | 品質ルール定義 |
| **5. 実装** | 変換ロジック、スケジュール | `pipeline-scaffold`, `data-contract`, `/etl-tdd` | パイプラインコード |
| **6. テスト** | テストケース、エッジケース | `etl-tdd-guide`, `test-data-generator` | テストコード、エビデンス |
| **7. 運用** | 監視要件、障害シナリオ | `sla-validator`, `incident-runbook`, `/lineage` | ランブック、リネージ図 |

---

## 使い方

### 最小スタート（まず Phase 1 だけ埋める）

1. このファイルの Phase 1 セクションを YAML テンプレートとしてコピー
2. わかる範囲で埋める（空欄 OK）
3. Claude Code に渡す → `pipeline-architect` が不足情報を質問してくれる

### フル定義（設計をしっかりやる場合）

1. Phase 1〜4 をすべて埋める
2. Claude Code に渡す → 各エージェントが設計レビュー
3. Phase 5〜7 は設計結果を基に自動生成 + レビュー

### 既存パイプラインの改善

1. Phase 3（既存スキーマ）と Phase 4（品質ルール）から始める
2. `/pipeline-review` で現状を診断
3. 問題箇所を特定してから Phase 2 に戻って再設計
