---
name: test-data-generator
description: Generate realistic test data using Faker with edge cases, anonymization, and fixed seeds for reproducible ETL testing.
---

# Test Data Generator

Faker ベースのテストデータ生成ツール。固定シードによる再現性、エッジケースの自動生成、PII マスキングを提供する。

---

## 基本方針

| 方針 | 説明 |
|------|------|
| 再現性 | 固定シード（Seed）により、同じデータを何度でも生成可能 |
| エッジケース網羅 | NULL、空文字、境界値、Unicode、特殊文字を自動的に含む |
| 匿名化 | 個人情報（PII）は生成時点でフェイクデータを使用 |
| スキーマ準拠 | データコントラクトやスキーマ定義に基づいてデータを生成 |
| PySpark 対応 | PySpark DataFrame として直接生成可能 |

---

## 固定シードによる再現性

テストの再現性を確保するため、必ず固定シードを使用する。

```python
from faker import Faker
import random

SEED = 42

def create_faker(locale: str = "ja_JP", seed: int = SEED) -> Faker:
    """再現性のある Faker インスタンスを生成する"""
    fake = Faker(locale)
    Faker.seed(seed)
    random.seed(seed)
    return fake
```

**シード値の管理ルール**:
- テストモジュールごとに異なるシード値を使用する
- シード値は定数として定義し、テストファイルの先頭に記載する
- CI/CD で使用するシード値は設定ファイルで管理する

---

## エッジケースデータの自動生成

ETL テストでは、以下のエッジケースを必ずテストデータに含める。

| カテゴリ | エッジケース | 例 |
|---------|-------------|-----|
| NULL | NULL 値 | `None` |
| 空文字 | 空文字列 | `""` |
| 空白 | 空白のみ | `"   "`, `"\t"`, `"\n"` |
| 境界値 | 最小値・最大値 | `0`, `2147483647`, `-2147483648` |
| 日付境界 | 年末年始・閏年 | `2024-02-29`, `2024-12-31` |
| Unicode | マルチバイト文字 | `"田中太郎"`, `"O'Brien"`, `"Muller"` |
| 特殊文字 | SQL インジェクション風 | `"'; DROP TABLE --"` |
| 長い文字列 | 最大長超過 | `"a" * 10000` |
| 数値精度 | 浮動小数点 | `0.1 + 0.2`, `float('inf')` |
| 重複 | 重複レコード | 同一 ID のレコード |

---

## Python Faker + PySpark テストデータ生成例

```python
"""
Test Data Generator for ETL Pipeline Testing
"""
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from decimal import Decimal
from faker import Faker
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DecimalType, DateType, TimestampType, BooleanType,
)
import random
import logging

logger = logging.getLogger(__name__)

SEED = 42


@dataclass(frozen=True)
class GenerationConfig:
    """テストデータ生成設定"""
    num_records: int = 1000
    null_rate: float = 0.05
    duplicate_rate: float = 0.02
    edge_case_rate: float = 0.1
    locale: str = "ja_JP"
    seed: int = SEED


@dataclass
class GenerationResult:
    """テストデータ生成結果"""
    total_records: int
    null_records: int
    edge_case_records: int
    duplicate_records: int
    schema: StructType


def create_order_test_data(
    spark: SparkSession,
    config: GenerationConfig = GenerationConfig(),
) -> DataFrame:
    """注文テストデータを生成する"""
    fake = Faker(config.locale)
    Faker.seed(config.seed)
    random.seed(config.seed)

    schema = StructType([
        StructField("order_id", StringType(), False),
        StructField("customer_id", StringType(), True),
        StructField("order_date", DateType(), True),
        StructField("total_amount", DecimalType(10, 2), True),
        StructField("status", StringType(), True),
        StructField("shipping_address", StringType(), True),
        StructField("updated_at", TimestampType(), True),
    ])

    statuses = ["PENDING", "CONFIRMED", "SHIPPED", "DELIVERED", "CANCELLED"]
    records = []

    for i in range(config.num_records):
        is_null = random.random() < config.null_rate
        is_edge = random.random() < config.edge_case_rate

        if is_edge:
            record = _generate_edge_case_order(fake, i, statuses)
        elif is_null:
            record = _generate_null_order(fake, i, statuses)
        else:
            record = _generate_normal_order(fake, i, statuses)

        records.append(record)

    # 重複レコードの追加
    num_duplicates = int(config.num_records * config.duplicate_rate)
    if records and num_duplicates > 0:
        duplicates = random.choices(records, k=num_duplicates)
        records.extend(duplicates)

    df = spark.createDataFrame(records, schema=schema)

    logger.info("Test data generated", extra={
        "total_records": len(records),
        "null_rate": config.null_rate,
        "edge_case_rate": config.edge_case_rate,
        "duplicate_count": num_duplicates,
    })

    return df


def _generate_normal_order(
    fake: Faker, index: int, statuses: list[str]
) -> tuple:
    """通常の注文レコードを生成する"""
    return (
        fake.uuid4(),
        f"CUST-{fake.random_int(min=1000, max=9999)}",
        fake.date_between(start_date="-1y", end_date="today"),
        Decimal(str(round(random.uniform(100, 100000), 2))),
        random.choice(statuses),
        fake.address(),
        datetime.now() - timedelta(hours=random.randint(0, 720)),
    )


def _generate_null_order(
    fake: Faker, index: int, statuses: list[str]
) -> tuple:
    """NULL を含む注文レコードを生成する"""
    return (
        fake.uuid4(),
        None,  # customer_id が NULL
        fake.date_between(start_date="-1y", end_date="today"),
        None,  # total_amount が NULL
        random.choice(statuses),
        None,  # shipping_address が NULL
        datetime.now(),
    )


def _generate_edge_case_order(
    fake: Faker, index: int, statuses: list[str]
) -> tuple:
    """エッジケースの注文レコードを生成する"""
    edge_cases = [
        # 金額が0
        (fake.uuid4(), f"CUST-0000", date(2024, 2, 29),
         Decimal("0.00"), "PENDING", "", datetime.now()),
        # 非常に高額
        (fake.uuid4(), f"CUST-9999", date(2024, 12, 31),
         Decimal("99999999.99"), "CONFIRMED", "a" * 500, datetime.now()),
        # 特殊文字を含む住所
        (fake.uuid4(), f"CUST-{index}", date.today(),
         Decimal("1000.00"), "SHIPPED", "'; DROP TABLE orders --",
         datetime.now()),
        # Unicode 文字
        (fake.uuid4(), f"CUST-{index}", date.today(),
         Decimal("5000.00"), "DELIVERED",
         "東京都千代田区丸の内1-1-1", datetime.now()),
    ]
    return random.choice(edge_cases)
```

---

## 匿名化パターン（PII マスキング）

本番データをテストに使用する場合は、以下のパターンで匿名化する。

| PII 項目 | 匿名化方法 | 例 |
|---------|-----------|-----|
| 氏名 | Faker で置換 | `田中太郎` -> `佐藤花子` |
| メールアドレス | ドメイン保持でローカル部を置換 | `user@example.com` -> `fake123@example.com` |
| 電話番号 | 先頭3桁保持で残りをランダム化 | `090-1234-5678` -> `090-9876-5432` |
| 住所 | 都道府県保持で以降を置換 | `東京都...` -> `東京都...（Faker生成）` |
| クレジットカード | 下4桁以外をマスク | `4111-1111-1111-1234` -> `****-****-****-1234` |
| 生年月日 | 年齢層を保持してランダム化 | `1990-05-15` -> `1991-08-22` |

```python
def anonymize_dataframe(df: DataFrame) -> DataFrame:
    """DataFrame の PII カラムを匿名化する"""
    from pyspark.sql import functions as F

    return (
        df
        .withColumn("customer_name", F.lit("ANONYMIZED"))
        .withColumn("email",
            F.concat(
                F.expr("uuid()"),
                F.lit("@example.com")
            )
        )
        .withColumn("phone",
            F.concat(
                F.substring("phone", 1, 3),
                F.lit("-"),
                F.expr("CAST(FLOOR(RAND() * 9000 + 1000) AS STRING)"),
                F.lit("-"),
                F.expr("CAST(FLOOR(RAND() * 9000 + 1000) AS STRING)"),
            )
        )
    )
```

---

## テストデータ生成の実行手順

1. スキーマ定義またはデータコントラクトを入力として受け取る
2. GenerationConfig でパラメータを設定する
3. 通常データ + エッジケース + NULL + 重複データを生成する
4. PySpark DataFrame として返す
5. 生成統計をログに出力する

---

## 注意事項

- テストデータに本番データを使用しない（Faker で生成する）
- 固定シードを必ず設定し、テストの再現性を確保する
- エッジケースのカバレッジを定期的に見直す
- 大量データのパフォーマンステストには、別途専用のデータ生成スクリプトを用意する
