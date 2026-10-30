# ライブ実演台本 — 工場生産実績 ETL パイプライン

| 項目 | 値 |
|------|-----|
| 所要時間 | 15〜20 分 |
| 対象者 | エンジニア（ETL / データ基盤に興味がある方） |
| 前提 | Claude Code + tf-etl-env がインストール済み |

---

## 導入（2 分）

### 話すこと

> 「tf-etl-env は、ETL パイプライン開発を Claude Code で支援する標準環境です。
> 要件定義からアーキテクチャ設計、実装、テスト、運用設計まで、
> 7 つのフェーズを専用エージェントとルールでカバーします。
>
> 今日は、製造業の工場で日次出力される生産実績 CSV を
> Azure Fabric Lakehouse に取り込み、BI で可視化するシナリオをデモします。」

### 見せるもの

- `demo/manufacturing-iot/` のディレクトリ構成
- `docs/ETL-REQUIREMENTS-GUIDE.md` の 7 フェーズ概要

---

## Phase 1: 要件定義（2 分）

### プロンプト例

```
工場の生産実績データを Lakehouse に取り込むパイプラインを作りたい。
- ソース: MES から日次で CSV 出力（500-1000 件/日）
- ターゲット: Azure Fabric Lakehouse → Power BI
- SLA: 毎朝 AM 8:00 に前日データが見えること
要件を整理してください。
```

### 見どころ

- YAML 形式の要件テンプレート生成
- ⚡推論マークによる不足情報の明示
- 確認ポイントの自動抽出

### 期待される出力

→ `01_requirements/requirements.yaml` を表示

---

## Phase 2: アーキテクチャ設計（3 分）

### プロンプト例

```
/pipeline-review
この要件に基づいてパイプラインのアーキテクチャを設計してください。
Medallion Architecture（Bronze → Silver → Gold）で構成したい。
```

### 見どころ

- **pipeline-architect エージェント**が自動起動
- ✅確定 / ⚡推論 マークの使い分け
- ADR（Architecture Decision Record）の自動生成
- Mermaid フロー図の生成

### 期待される出力

→ `02_architecture/architecture.md` を表示
→ `02_architecture/ADR-001_pipeline-pattern.md` の比較表を表示
→ `02_architecture/diagrams/pipeline-flow.mmd` を Mermaid プレビュー

### ポイントを強調

> 「5 つの代替案を比較して ADR を自動生成しています。
> 推論した部分は ⚡ マークで明示し、確認を求めます。
> これにより、設計判断のトレーサビリティが確保されます。」

---

## Phase 3: スキーマ設計（2 分）

### プロンプト例

```
/schema-review
Medallion の各レイヤーのスキーマを設計してください。
Bronze は全 STRING、Silver は型変換・品質チェック済み、Gold は集計テーブル。
```

### 見どころ

- **schema-designer エージェント**による DDL 生成
- ER 図（Mermaid）の自動生成
- SCD Type 2（設備マスタ）の設計提案

### 期待される出力

→ `03_schema/ddl/silver.sql` を表示
→ `03_schema/diagrams/er-diagram.mmd` を Mermaid プレビュー

---

## Phase 4: 品質ルール設計（2 分）

### プロンプト例

```
/dq-check
データ品質ルールを 6 次元（完全性・正確性・一貫性・適時性・一意性・有効性）で設計してください。
品質ゲートの閾値も決めたい。
```

### 見どころ

- **data-quality-reviewer エージェント**による品質ルール生成
- 6 次元品質フレームワークの適用
- データ契約（Producer ↔ Consumer）の定義

### 期待される出力

→ `04_quality/quality-rules.yaml` の 6 次元ルール
→ `04_quality/data-contract.yaml` のデータ契約

### ポイントを強調

> 「品質ルールはコードに先んじて設計します。
> これにより、テストファーストのアプローチが可能になります。」

---

## Phase 5: パイプライン実装（3 分）

### プロンプト例

```
設計に基づいて PySpark のパイプラインコードを実装してください。
Extract（CSV → Bronze）、Transform（Bronze → Silver）、Aggregate（Silver → Gold）の 3 ステップ。
冪等性を確保して、再実行安全にしてください。
```

### 見どころ

- **冪等性パターン**（`replaceWhere` によるパーティション上書き）
- Dead Letter テーブルへのエラーデータ分離
- Frozen dataclass による処理統計（イミュータブル）
- 構造化ログ（JSON 形式）

### 期待される出力

→ `05_implementation/src/transform.py` の品質チェックロジック（`validate_quality`）
→ `05_implementation/src/pipeline.py` のオーケストレーション

### コードを見せながら説明

```python
# transform.py の validate_quality を表示
# 「4 つの品質チェック（必須項目・範囲・Enum・一貫性）を結合して、
#   合格/不合格に分離しています。不合格は Dead Letter に送られます。」
```

---

## Phase 6: テスト実装（3 分）

### プロンプト例

```
/etl-tdd
パイプラインのテストを書いてください。
品質ゲートのテストを重点的に。
```

### 見どころ

- **etl-tdd-guide エージェント**による TDD ワークフロー
- 6 次元品質ルールの各テスト
- エッジケース（NULL、境界値、空文字）のテスト
- サンプル CSV を使った統合テスト

### 期待される出力

→ `06_testing/test_quality_gates.py` のテストクラス構造
→ テスト実行結果（GREEN）

### ポイントを強調

> 「品質ルール 1 つ 1 つに対応するテストがあります。
> 温度の範囲チェック（20〜80 度）、ステータスの Enum チェック、
> 不良数 ≤ 実績数の一貫性チェック。
> これが TDD（テストファースト）の ETL 版です。」

---

## Phase 7: 運用設計（2 分）

### プロンプト例

```
/lineage
運用設計をしてください。障害対応ランブック、SLA 定義、データリネージ図。
```

### 見どころ

- **sla-validator エージェント**による SLA 定義
- 3 シナリオの障害対応ランブック
- データリネージ図（Mermaid）

### 期待される出力

→ `07_operations/diagrams/lineage.mmd` を Mermaid プレビュー
→ `07_operations/runbook.md` のシナリオ 2（品質スコア超過）

### ポイントを強調

> 「運用設計もコードと同時に生成します。
> ランブックには診断 SQL と復旧手順が含まれています。
> 冪等性のおかげで、いつでも安全に再実行できます。」

---

## まとめ（1 分）

### 話すこと

> 「今日のデモでは、7 つのフェーズを通して、
> 要件定義から運用設計まで一気通貫で ETL パイプラインを構築しました。
>
> tf-etl-env の特徴をまとめると:
>
> 1. **専用エージェント 6 体**がフェーズごとに支援
> 2. **10 のルール**が設計原則を自動適用
> 3. **⚡推論マーク**で不足情報を明示、確認を促進
> 4. **ADR**で設計判断を自動記録
> 5. **冪等性・品質ゲート・Dead Letter**で信頼性を確保
> 6. **テストファースト**でデータ品質を保証
>
> ETL 開発の標準化と品質向上に貢献します。」

---

## 付録: tf-etl-env コンポーネント使用マップ

| Phase | エージェント | コマンド | スキル | ルール |
|-------|-------------|---------|--------|--------|
| 1 要件定義 | — | — | — | — |
| 2 アーキテクチャ | pipeline-architect | /pipeline-review | — | pipeline-patterns, medallion-architecture |
| 3 スキーマ | schema-designer | /schema-review | — | schema-evolution, partitioning |
| 4 品質 | data-quality-reviewer | /dq-check | data-contract | data-quality |
| 5 実装 | — | — | pipeline-scaffold | idempotency-patterns |
| 6 テスト | etl-tdd-guide | /etl-tdd | test-data-generator | etl-testing |
| 7 運用 | sla-validator | /lineage | incident-runbook | sla-management, lineage |

**カバー率**: Agent 5/6, Skill 4/7, Command 5/5, Rule 9/10

---

## トラブルシューティング

| 問題 | 対処 |
|------|------|
| エージェントが起動しない | `~/.claude/agents/` にファイルが存在するか確認 |
| Mermaid プレビューが表示されない | VSCode Mermaid 拡張をインストール |
| テストが失敗する | `pyspark`, `delta-spark` パッケージの確認 |
| CSV の文字化け | `encoding: "utf-8-sig"` を指定 |
