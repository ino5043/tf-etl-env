---
description: Review schema design for normalization, partitioning, indexing, and evolution compatibility.
---

# Schema Review

schema-designer エージェントを起動し、テーブルスキーマの設計レビューを実行する。

---

## 実行手順

### 1. レビュー対象の指定

```bash
/schema-review path/to/schema.sql
/schema-review path/to/models/staging/
```

対応形式:
- SQL DDL ファイル
- dbt model ファイル（.sql + schema.yml）
- Delta Table スキーマ定義
- データコントラクト YAML

### 2. レビュー観点

schema-designer エージェントが以下の観点でレビューを実行する。

| 観点 | チェック内容 |
|------|-------------|
| 正規化 | 適切な正規形か（1NF〜3NF）、過度な正規化・非正規化がないか |
| パーティション | パーティションキーの選択が適切か、パーティション数が妥当か |
| インデックス | クエリパターンに基づくインデックス設計が適切か |
| SCD (Slowly Changing Dimensions) | SCD Type 1/2/3 の選択が要件に合っているか |
| 命名規約 | テーブル名・カラム名がプロジェクトの命名規約に従っているか |
| データ型 | カラムのデータ型が適切か（精度、サイズ） |
| 制約 | NOT NULL, UNIQUE, CHECK 制約が適切に設定されているか |
| スキーマ進化 | 将来のスキーマ変更に対する互換性が確保されているか |

### 3. SCD 妥当性チェック

| SCD Type | ユースケース | トレードオフ |
|----------|-------------|-------------|
| Type 1 (上書き) | 修正データ、最新値のみ必要 | 履歴なし |
| Type 2 (履歴保持) | 変更履歴が必要、監査要件 | ストレージ増加 |
| Type 3 (前回値保持) | 直前の値のみ必要 | 2世代分のみ |

---

## 出力

```markdown
# Schema Review Report

## 対象スキーマ
- テーブル数: N
- 対象レイヤー: Bronze / Silver / Gold

## レビュー結果
| 観点 | 評価 | 指摘事項 |
|------|------|---------|
| 正規化 | OK / NG | [詳細] |
| パーティション | OK / NG | [詳細] |
| インデックス | OK / NG | [詳細] |
| SCD | OK / NG | [詳細] |
| 命名規約 | OK / NG | [詳細] |
| データ型 | OK / NG | [詳細] |
| 制約 | OK / NG | [詳細] |
| スキーマ進化 | OK / NG | [詳細] |

## 改善推奨事項
1. [推奨事項]
```

---

## スキーマ進化の互換性確認

以下のルールに基づいて、スキーマ変更の互換性を判定する。

| 変更操作 | 後方互換 | 前方互換 | 判定 |
|---------|---------|---------|------|
| カラム追加（Optional） | Yes | Yes | 安全 |
| カラム追加（Required） | No | Yes | 要マイグレーション |
| カラム削除 | Yes | No | 要調整 |
| カラム名変更 | No | No | 破壊的変更 |
| 型の拡張（int -> bigint） | Yes | Yes | 安全 |
| 型の縮小（bigint -> int） | No | No | 破壊的変更 |

---

## 注意事項

- Medallion レイヤーごとにスキーマ設計の基準が異なる（Bronze は生データ保持優先、Gold は分析最適化優先）
- パーティションキーはクエリパターンに基づいて選択する
- スキーマ変更時は `/migration-plan` スキルと併用して影響分析を行う
