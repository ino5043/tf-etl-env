---
description: Start ETL test-driven development workflow with test data generation and quality-first testing.
---

# ETL TDD

etl-tdd-guide エージェントを起動し、ETL パイプラインのテスト駆動開発ワークフローを実行する。

---

## TDD サイクル

ETL 開発では以下のサイクルを繰り返す。

```
1. テストデータ生成 (Arrange)
   └→ /test-data-generator スキルでテストデータを作成

2. テスト作成 (RED)
   └→ 期待する変換結果を定義するテストを書く

3. テスト実行 → 失敗を確認
   └→ テストが正しく失敗することを確認

4. パイプライン実装 (GREEN)
   └→ テストが通る最小限の実装を行う

5. テスト実行 → 成功を確認
   └→ すべてのテストが通ることを確認

6. リファクタリング (IMPROVE)
   └→ コード品質を改善（テストは引き続き通ること）

7. カバレッジ確認
   └→ 80% 以上のカバレッジを確保
```

---

## 実行手順

### 1. テストフレームワークのセットアップ

```bash
/etl-tdd setup
```

以下のファイルが生成される:

```
tests/
├── conftest.py          # SparkSession フィクスチャ
├── test_extract.py      # 抽出テスト
├── test_transform.py    # 変換テスト
├── test_load.py         # ロードテスト
└── test_quality.py      # 品質テスト
```

### 2. conftest.py テンプレート

```python
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """テスト用 SparkSession"""
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("etl-test")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
        .getOrCreate()
    )
    yield session
    session.stop()
```

### 3. テスト作成の開始

```bash
/etl-tdd start --pipeline daily_orders
```

etl-tdd-guide エージェントが以下を支援する:
- テストケースの洗い出し（正常系、異常系、エッジケース）
- テストデータの生成（`/test-data-generator` スキル連携）
- アサーションの設計
- テスト実行とフィードバック

---

## テストカテゴリ

| カテゴリ | テスト内容 | 優先度 |
|---------|-----------|--------|
| スキーマテスト | 出力スキーマが期待通りか | 高 |
| 変換ロジックテスト | ビジネスロジックの正確性 | 高 |
| NULL ハンドリングテスト | NULL 値の処理が正しいか | 高 |
| 重複排除テスト | 重複レコードが除去されるか | 中 |
| エッジケーステスト | 境界値、特殊文字の処理 | 中 |
| パフォーマンステスト | 処理時間が SLA 内か | 低 |
| 品質ゲートテスト | 品質チェックの閾値クリア | 高 |

---

## テスト実行

```bash
# 全テスト実行
python -m pytest tests/ -v --tb=short

# カバレッジ付き実行
python -m pytest tests/ \
  --cov=src \
  --cov-report=html:test-results/coverage-html \
  --cov-report=xml:test-results/coverage.xml \
  --junitxml=test-results/junit.xml \
  --html=test-results/report.html --self-contained-html \
  -v --tb=short

# 特定テストのみ実行
python -m pytest tests/test_transform.py -v -k "test_null_handling"
```

---

## 注意事項

- テストは必ず実装 **前** に書く（TDD の原則）
- テスト用 SparkSession は `local[2]` で十分（本番設定をテストに持ち込まない）
- テストデータは Faker で生成し、固定シードで再現性を確保する
- カバレッジ 80% 未満ではマージを許可しない
