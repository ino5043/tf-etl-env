---
description: Run pipeline-architect and data-quality-reviewer agents in parallel for comprehensive ETL pipeline review.
---

# Pipeline Review

pipeline-architect と data-quality-reviewer エージェントを並列起動し、ETL パイプラインの包括的なレビューを実行する。

---

## 実行手順

以下の2つのエージェントを **並列** で起動する。

### Agent 1: pipeline-architect

パイプラインのアーキテクチャ観点でレビューする。

**レビュー観点**:
- Medallion レイヤー構成の妥当性
- 冪等性の確保（DELETE-INSERT, MERGE パターン）
- エラーハンドリングとリトライ戦略
- 依存関係の適切性（DAG 構造）
- リソース設定の妥当性（Spark 設定, メモリ）
- スケーラビリティ（データ量増加時の耐性）
- 監視・アラートの設計

### Agent 2: data-quality-reviewer

データ品質の観点でレビューする。

**レビュー観点**:
- データ品質チェックの網羅性（NULL, 一意性, 値域）
- データコントラクトとの整合性
- エラーデータの分離・保存（削除せず分離しているか）
- 処理統計のログ出力（件数, エラー率, 処理時間）
- スキーマバリデーションの有無
- SLA 準拠の確認

---

## 入力

レビュー対象のパイプラインソースコードを指定する。

```bash
/pipeline-review src/pipelines/daily_orders.py
```

対応形式:
- Python（PySpark, Airflow DAG）
- SQL（dbt models, ストアドプロシージャ）
- JSON（ADF Pipeline 定義）

---

## 出力

統合レビューレポートを以下の形式で出力する。

```markdown
# Pipeline Review Report

## Architecture Review (pipeline-architect)
| 観点 | 評価 | 指摘事項 |
|------|------|---------|
| 冪等性 | OK / NG | [詳細] |
| エラーハンドリング | OK / NG | [詳細] |
| スケーラビリティ | OK / NG | [詳細] |

## Data Quality Review (data-quality-reviewer)
| 観点 | 評価 | 指摘事項 |
|------|------|---------|
| 品質チェック網羅性 | OK / NG | [詳細] |
| エラーデータ分離 | OK / NG | [詳細] |
| 処理統計ログ | OK / NG | [詳細] |

## 総合評価
- Critical: N 件
- High: N 件
- Medium: N 件
- Low: N 件

## 改善推奨事項
1. [推奨事項1]
2. [推奨事項2]
```

---

## 注意事項

- 両エージェントのレビュー結果を統合し、重複する指摘は集約する
- Critical / High の指摘がある場合は、修正が完了するまでマージしない
- レビュー結果は PR コメントまたはレビューレポートとして記録する
