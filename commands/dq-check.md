---
description: Generate and review data quality check definitions using Great Expectations or dbt tests.
---

# DQ Check

データ品質チェック定義の生成・レビューを行う。Great Expectations スイートまたは dbt tests を対象とする。

---

## 実行手順

### 1. 品質チェックフレームワークの選択

| フレームワーク | ユースケース | 出力形式 |
|---------------|-------------|---------|
| Great Expectations | Python パイプライン, Notebook | Expectation Suite (JSON) |
| dbt tests | dbt プロジェクト | YAML テスト定義 |

### 2. チェック定義の生成

対象テーブルのスキーマに基づいて、以下の品質チェックを自動生成する。

| チェック種別 | 説明 | 例 |
|-------------|------|-----|
| not_null | NULL でないこと | 必須カラム全体 |
| unique | 一意であること | PK, ナチュラルキー |
| accepted_values | 許容値リストに含まれること | ステータスカラム |
| relationships | 外部キー整合性 | 参照先テーブルに存在 |
| expression | カスタム条件 | `amount >= 0` |
| freshness | データの鮮度 | 最新データが N 時間以内 |
| volume | レコード数の妥当性 | 前日比 +-50% 以内 |

### 3. Great Expectations スイート生成

```bash
/dq-check --framework ge --table orders --output expectations/orders_suite.json
```

生成される Expectation Suite の例:
```json
{
  "expectation_suite_name": "orders_quality_suite",
  "expectations": [
    {
      "expectation_type": "expect_column_values_to_not_be_null",
      "kwargs": {"column": "order_id"}
    },
    {
      "expectation_type": "expect_column_values_to_be_unique",
      "kwargs": {"column": "order_id"}
    }
  ]
}
```

### 4. dbt tests 生成

```bash
/dq-check --framework dbt --table orders --output models/staging/schema.yml
```

生成される dbt tests の例:
```yaml
models:
  - name: stg_orders
    columns:
      - name: order_id
        tests:
          - not_null
          - unique
      - name: status
        tests:
          - accepted_values:
              values: ['PENDING', 'CONFIRMED', 'SHIPPED']
```

---

## 品質ダッシュボード設計支援

生成された品質チェック定義に基づいて、以下のダッシュボード項目を提案する。

| メトリクス | 可視化 | アラート閾値 |
|-----------|--------|------------|
| テーブル別 NULL 率 | 折れ線グラフ（時系列） | > 5% |
| カラム別カーディナリティ推移 | 棒グラフ | 前日比 +-30% |
| レコード数推移 | 折れ線グラフ | 前日比 +-50% |
| 品質チェック合格率 | ゲージ | < 95% |
| データ鮮度 | ステータス表示 | > SLA 時間 |

---

## 注意事項

- 品質チェックの閾値はデータプロファイリング結果に基づいて設定する
- チェック定義はバージョン管理リポジトリで管理する
- CI/CD パイプラインに品質チェックを組み込み、違反時にパイプラインを停止する
- 品質チェックの実行結果は構造化ログで出力する
