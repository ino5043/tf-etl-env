# Manufacturing IoT Demo — 工場生産実績 ETL パイプライン

tf-etl-env の全機能を紹介するショーケースデモ。
製造業の工場で日次出力される生産実績 CSV を Azure Fabric Lakehouse に取り込み、
品質チェック・異常検知を経て Power BI ダッシュボードで工場長が AM 8:00 に前日の生産状況を確認できるシナリオ。

## シナリオ概要

```
MES (CSV) → Bronze (全STRING) → Silver (型変換・品質チェック) → Gold (KPI集計) → Power BI
```

| 項目 | 値 |
|------|-----|
| ソース | 製造実行システム (MES) — 日次 CSV |
| データ量 | 500〜1,000 件/日 |
| ターゲット | Azure Fabric Lakehouse (Delta Lake) |
| SLA | 毎日 AM 8:00 JST |
| エラー率閾値 | < 1% |

## ディレクトリ構成

```
demo/manufacturing-iot/
├── README.md                    # 本ファイル
├── DEMO-SCRIPT.md               # ライブ実演台本（15-20分）
│
├── 01_requirements/             # Phase 1: 要件定義
│   └── requirements.yaml
│
├── 02_architecture/             # Phase 2: アーキテクチャ設計
│   ├── architecture.md
│   ├── ADR-001_pipeline-pattern.md
│   └── diagrams/
│       └── pipeline-flow.mmd
│
├── 03_schema/                   # Phase 3: スキーマ設計
│   ├── schema-design.md
│   ├── ddl/
│   │   ├── bronze.sql
│   │   ├── silver.sql
│   │   └── gold.sql
│   └── diagrams/
│       └── er-diagram.mmd
│
├── 04_quality/                  # Phase 4: 品質設計
│   ├── quality-rules.yaml
│   ├── data-contract.yaml
│   └── quality-design.md
│
├── 05_implementation/           # Phase 5: 実装
│   ├── config/
│   │   └── pipeline_config.yaml
│   └── src/
│       ├── __init__.py
│       ├── extract.py           # Bronze: CSV 取り込み
│       ├── transform.py         # Silver: 型変換・品質チェック
│       ├── aggregate.py         # Gold: KPI 集計
│       └── pipeline.py          # オーケストレーション
│
├── 06_testing/                  # Phase 6: テスト
│   ├── conftest.py
│   ├── test_extract.py
│   ├── test_transform.py
│   ├── test_aggregate.py
│   ├── test_quality_gates.py
│   ├── test_fabric_notebook.py  # 08_fabric Notebook のスモークテスト
│   └── sample_data/
│       ├── generate_test_data.py
│       ├── production_normal.csv
│       ├── production_edge.csv
│       └── production_error.csv
│
├── 07_operations/               # Phase 7: 運用設計
│   ├── sla-definition.yaml
│   ├── runbook.md
│   ├── monitoring-design.md
│   └── diagrams/
│       └── lineage.mmd
│
├── 08_fabric/                   # Phase 8: Fabric デプロイ
│   ├── deploy-guide.md          # 試用版での配置手順・検証 SQL・キャプチャ一覧
│   ├── notebook/
│   │   ├── fabric_pipeline.py   # Fabric Notebook（Bronze → Silver → Gold + dim_*）
│   │   ├── fabric_pipeline.ipynb
│   │   └── build_ipynb.py       # .py → .ipynb 変換
│   ├── sample_data/
│   │   ├── generate_fabric_sample.py
│   │   ├── production_YYYYMMDD.csv  # 30 日分（2026-10-01〜30）
│   │   ├── line_master.csv
│   │   ├── product_master.csv
│   │   └── equipment_master_seed.csv
│   ├── powerbi/                 # セマンティックモデル・DAX・ダッシュボード仕様
│   ├── design/                  # 移行・セキュリティ設計
│   └── diagrams/
│       └── fabric-deploy-flow.mmd
│
└── showcase/                    # ショーケース素材
    ├── index.html               # ランディングページ（自社サイト誘導用）
    └── highlights.md
```

## 前提条件

- Python 3.12（CPython 公式版を推奨。Microsoft Store 版は PySpark と非互換）
- JDK 17（Eclipse Temurin 推奨）
- PySpark 3.5+
- delta-spark 3.2+
- pytest, pandas, pyarrow
- Claude Code + tf-etl-env (tf-standard-env >= 2.0.0)

## テスト実行

```bash
# venv を作成（Python 3.12 推奨）
python -m venv .venv
source .venv/Scripts/activate  # Windows
pip install pyspark==3.5.6 delta-spark==3.2.1 pytest pandas pyarrow setuptools

# JAVA_HOME を JDK 17 に設定
export JAVA_HOME="/c/Program Files/Eclipse Adoptium/jdk-17.0.18.8-hotspot"

# テスト実行
cd demo/manufacturing-iot
python -m pytest 06_testing/ -v

# カバレッジ付き
pip install pytest-cov
python -m pytest 06_testing/ -v --cov=05_implementation/src --cov-report=html
```

## tf-etl-env コンポーネント使用マップ

| Phase | エージェント | コマンド | ルール |
|-------|-------------|---------|--------|
| 2 アーキテクチャ | pipeline-architect | /pipeline-review | pipeline-patterns, medallion-architecture |
| 3 スキーマ | schema-designer | /schema-review | schema-evolution, partitioning |
| 4 品質 | data-quality-reviewer | /dq-check | data-quality |
| 5 実装 | — | — | idempotency-patterns |
| 6 テスト | etl-tdd-guide | /etl-tdd | etl-testing |
| 7 運用 | sla-validator | /lineage | sla-management, lineage |
| 8 Fabric デプロイ | — | — | idempotency-patterns, medallion-architecture |

**カバー率**: Agent 5/6, Command 5/5, Rule 9/10

## ショーケース

`showcase/index.html` を自社サイトから iframe 埋め込みまたはリンクして使用できます。

```html
<!-- iframe 埋め込み -->
<iframe src="showcase/index.html" width="100%" height="800" frameborder="0"></iframe>

<!-- または直リンク -->
<a href="showcase/index.html">デモの詳細を見る</a>
```

GitHub Pages で公開する場合: Settings > Pages > Source を `main` / `/ (root)` に設定すると
`https://<org>.github.io/tf-etl-env/demo/manufacturing-iot/showcase/` でアクセス可能になります。

## 設計上の特徴

### 冪等性

すべての書き込みは `replaceWhere` によるパーティション上書き。再実行してもデータ重複なし。

### 品質ゲート

6 次元品質チェック（完全性・正確性・一貫性・適時性・一意性・有効性）。
不合格レコードは Dead Letter テーブルに分離。

### 構造化ログ

すべてのログイベントは JSON 形式で `event` キーを持つ。障害調査を容易にする。

### ADR

アーキテクチャ判断を ADR で記録。5 つの代替案を比較し、意思決定のトレーサビリティを確保。
