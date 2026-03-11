# データ品質の6次元と検証ルール

データ品質を体系的に管理するためのルール。6つの品質次元に基づいてデータを検証し、品質スコアを算出する。

---

## 1. 完全性 (Completeness)

必要なデータが欠けていないかを検証する。

### 1.1 NOT NULL チェック

```python
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, when, isnan, isnull

def check_completeness(df: DataFrame, required_columns: list[str]) -> dict:
    """必須カラムの NULL / NaN チェック"""
    total_rows = df.count()
    results = {}

    for column in required_columns:
        null_count = df.filter(
            isnull(col(column)) | isnan(col(column))
        ).count()

        results[column] = {
            "total": total_rows,
            "null_count": null_count,
            "completeness_rate": round((total_rows - null_count) / total_rows * 100, 2)
        }

    return results
```

### 1.2 レコード件数比較

```python
def check_record_count(
    source_count: int,
    target_count: int,
    tolerance_pct: float = 0.01
) -> dict:
    """ソースとターゲットのレコード件数を比較"""
    diff = abs(source_count - target_count)
    diff_pct = diff / source_count if source_count > 0 else 0

    return {
        "source_count": source_count,
        "target_count": target_count,
        "difference": diff,
        "difference_pct": round(diff_pct * 100, 4),
        "passed": diff_pct <= tolerance_pct,
        "threshold_pct": tolerance_pct * 100
    }
```

---

## 2. 正確性 (Accuracy)

データが現実世界の値を正しく反映しているかを検証する。

### 2.1 範囲チェック

```python
from pyspark.sql.functions import col

def check_range(df: DataFrame, column: str, min_val, max_val) -> dict:
    """値の範囲チェック"""
    total = df.count()
    out_of_range = df.filter(
        (col(column) < min_val) | (col(column) > max_val)
    ).count()

    return {
        "column": column,
        "min_allowed": min_val,
        "max_allowed": max_val,
        "out_of_range_count": out_of_range,
        "accuracy_rate": round((total - out_of_range) / total * 100, 2),
        "passed": out_of_range == 0
    }

# 使用例
result = check_range(df, "temperature", min_val=-50, max_val=60)
result = check_range(df, "age", min_val=0, max_val=150)
```

### 2.2 マスタ照合

```python
def check_referential_accuracy(
    fact_df: DataFrame,
    master_df: DataFrame,
    fact_key: str,
    master_key: str
) -> dict:
    """ファクトテーブルのキーがマスタに存在するか照合"""
    total = fact_df.count()

    orphans = fact_df.join(
        master_df,
        fact_df[fact_key] == master_df[master_key],
        "left_anti"
    )
    orphan_count = orphans.count()

    return {
        "fact_key": fact_key,
        "master_key": master_key,
        "total_records": total,
        "orphan_count": orphan_count,
        "accuracy_rate": round((total - orphan_count) / total * 100, 2),
        "passed": orphan_count == 0,
        "orphan_samples": orphans.limit(10).collect() if orphan_count > 0 else []
    }
```

---

## 3. 一貫性 (Consistency)

複数のデータソースやカラム間で矛盾がないかを検証する。

### 3.1 クロスバリデーション

```python
def check_cross_field_consistency(df: DataFrame) -> dict:
    """フィールド間の整合性チェック"""
    total = df.count()

    # 例: 終了日は開始日より後でなければならない
    date_inconsistent = df.filter(
        col("end_date") < col("start_date")
    ).count()

    # 例: 合計 = 単価 x 数量
    calc_inconsistent = df.filter(
        abs(col("total_amount") - col("unit_price") * col("quantity")) > 0.01
    ).count()

    return {
        "date_consistency": {
            "inconsistent_count": date_inconsistent,
            "rate": round((total - date_inconsistent) / total * 100, 2)
        },
        "calculation_consistency": {
            "inconsistent_count": calc_inconsistent,
            "rate": round((total - calc_inconsistent) / total * 100, 2)
        }
    }
```

### 3.2 参照整合性

```sql
-- 参照整合性チェック: 注文明細の商品IDが商品マスタに存在するか
SELECT
    ol.order_id,
    ol.product_id,
    'ORPHAN_PRODUCT' AS issue_type
FROM order_lines ol
LEFT JOIN products p ON ol.product_id = p.product_id
WHERE p.product_id IS NULL;
```

---

## 4. 適時性 (Timeliness)

データが期待される時間内に利用可能かを検証する。

### 4.1 SLA チェック

```python
from datetime import datetime, timedelta

def check_timeliness(
    last_update: datetime,
    sla_hours: int,
    current_time: datetime = None
) -> dict:
    """データの鮮度チェック"""
    now = current_time or datetime.utcnow()
    age_hours = (now - last_update).total_seconds() / 3600

    return {
        "last_update": last_update.isoformat(),
        "current_time": now.isoformat(),
        "age_hours": round(age_hours, 2),
        "sla_hours": sla_hours,
        "passed": age_hours <= sla_hours,
        "overdue_hours": max(0, round(age_hours - sla_hours, 2))
    }
```

### 4.2 データ鮮度チェック

```python
def check_data_freshness(df: DataFrame, date_column: str, expected_date: str) -> dict:
    """最新データの日付が期待日付と一致するか"""
    from pyspark.sql.functions import max as spark_max

    latest_date = df.agg(spark_max(col(date_column))).collect()[0][0]

    return {
        "latest_date": str(latest_date),
        "expected_date": expected_date,
        "passed": str(latest_date) == expected_date,
        "gap_days": (
            datetime.strptime(expected_date, "%Y-%m-%d") -
            datetime.strptime(str(latest_date), "%Y-%m-%d")
        ).days if latest_date else None
    }
```

---

## 5. 一意性 (Uniqueness)

データに意図しない重複がないかを検証する。

### 5.1 主キー重複チェック

```python
from pyspark.sql.functions import count as spark_count

def check_uniqueness(df: DataFrame, key_columns: list[str]) -> dict:
    """主キーの一意性チェック"""
    total = df.count()
    distinct = df.select(key_columns).distinct().count()
    duplicate_count = total - distinct

    # 重複レコードのサンプル取得
    duplicates = (
        df.groupBy(key_columns)
        .agg(spark_count("*").alias("dup_count"))
        .filter(col("dup_count") > 1)
        .orderBy(col("dup_count").desc())
        .limit(10)
    )

    return {
        "key_columns": key_columns,
        "total_records": total,
        "distinct_records": distinct,
        "duplicate_count": duplicate_count,
        "uniqueness_rate": round(distinct / total * 100, 2) if total > 0 else 100,
        "passed": duplicate_count == 0,
        "duplicate_samples": duplicates.collect() if duplicate_count > 0 else []
    }
```

### 5.2 ファジーマッチ重複検出

```python
from pyspark.sql.functions import soundex, levenshtein, lower, trim

def detect_fuzzy_duplicates(
    df: DataFrame,
    name_column: str,
    threshold: int = 3
) -> DataFrame:
    """名前の類似度による重複候補の検出"""
    normalized = df.withColumn(
        "normalized_name",
        lower(trim(col(name_column)))
    ).withColumn(
        "name_soundex",
        soundex(col(name_column))
    )

    # 同じ Soundex コードを持つレコードをペアリング
    pairs = (
        normalized.alias("a")
        .join(
            normalized.alias("b"),
            (col("a.name_soundex") == col("b.name_soundex")) &
            (col("a.id") < col("b.id"))
        )
        .withColumn(
            "edit_distance",
            levenshtein(col("a.normalized_name"), col("b.normalized_name"))
        )
        .filter(col("edit_distance") <= threshold)
    )

    return pairs
```

---

## 6. 有効性 (Validity)

データが定義されたフォーマットやルールに従っているかを検証する。

### 6.1 正規表現チェック

```python
from pyspark.sql.functions import regexp_extract

VALIDATION_PATTERNS = {
    "email": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$",
    "phone_jp": r"^0\d{1,4}-?\d{1,4}-?\d{4}$",
    "postal_code_jp": r"^\d{3}-?\d{4}$",
    "date_iso": r"^\d{4}-\d{2}-\d{2}$",
}

def check_format_validity(
    df: DataFrame,
    column: str,
    pattern: str
) -> dict:
    """正規表現によるフォーマットチェック"""
    total = df.filter(col(column).isNotNull()).count()
    valid = df.filter(
        col(column).rlike(pattern)
    ).count()
    invalid = total - valid

    return {
        "column": column,
        "pattern": pattern,
        "total_non_null": total,
        "valid_count": valid,
        "invalid_count": invalid,
        "validity_rate": round(valid / total * 100, 2) if total > 0 else 100,
        "passed": invalid == 0
    }
```

### 6.2 Enum 値チェック

```python
def check_enum_values(
    df: DataFrame,
    column: str,
    allowed_values: list
) -> dict:
    """許可された値のみが含まれているか"""
    total = df.filter(col(column).isNotNull()).count()
    invalid = df.filter(
        ~col(column).isin(allowed_values) & col(column).isNotNull()
    )
    invalid_count = invalid.count()

    # 不正な値のサンプル
    invalid_samples = (
        invalid.groupBy(column)
        .count()
        .orderBy(col("count").desc())
        .limit(10)
        .collect()
    )

    return {
        "column": column,
        "allowed_values": allowed_values,
        "invalid_count": invalid_count,
        "validity_rate": round((total - invalid_count) / total * 100, 2) if total > 0 else 100,
        "passed": invalid_count == 0,
        "invalid_value_samples": invalid_samples
    }
```

---

## 7. Great Expectations でのルール定義

```python
import great_expectations as gx

context = gx.get_context()

# データソースの設定
datasource = context.data_sources.add_spark("spark_datasource")
data_asset = datasource.add_dataframe_asset("orders")

# Expectation Suite の定義
suite = context.suites.add(
    gx.ExpectationSuite(name="orders_quality_suite")
)

# 完全性
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToNotBeNull(column="order_id")
)
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToNotBeNull(column="customer_id")
)

# 一意性
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeUnique(column="order_id")
)

# 正確性 (範囲)
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeBetween(
        column="quantity", min_value=1, max_value=10000
    )
)

# 有効性 (Enum)
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeInSet(
        column="status",
        value_set=["PENDING", "CONFIRMED", "SHIPPED", "DELIVERED", "CANCELLED"]
    )
)

# 有効性 (正規表現)
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToMatchRegex(
        column="email", regex=r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
    )
)

# レコード件数
suite.add_expectation(
    gx.expectations.ExpectTableRowCountToBeBetween(
        min_value=1000, max_value=10000000
    )
)
```

---

## 8. 品質スコアリング

6次元の検証結果を統合して品質スコアを算出する。

```python
from dataclasses import dataclass

@dataclass(frozen=True)
class QualityDimension:
    name: str
    weight: float
    score: float  # 0.0 - 1.0

def calculate_quality_score(dimensions: list[QualityDimension]) -> dict:
    """加重平均による品質スコアの算出"""
    total_weight = sum(d.weight for d in dimensions)

    weighted_score = sum(
        d.score * d.weight for d in dimensions
    ) / total_weight

    return {
        "overall_score": round(weighted_score * 100, 2),
        "grade": _score_to_grade(weighted_score),
        "dimensions": {
            d.name: {
                "score": round(d.score * 100, 2),
                "weight": d.weight,
                "weighted_contribution": round(d.score * d.weight / total_weight * 100, 2)
            }
            for d in dimensions
        }
    }

def _score_to_grade(score: float) -> str:
    if score >= 0.95:
        return "A"
    elif score >= 0.90:
        return "B"
    elif score >= 0.80:
        return "C"
    elif score >= 0.70:
        return "D"
    else:
        return "F"

# 使用例
dimensions = [
    QualityDimension("completeness", weight=1.0, score=0.98),
    QualityDimension("accuracy", weight=1.0, score=0.95),
    QualityDimension("consistency", weight=0.8, score=0.99),
    QualityDimension("timeliness", weight=0.8, score=1.00),
    QualityDimension("uniqueness", weight=1.0, score=0.97),
    QualityDimension("validity", weight=0.8, score=0.96),
]

result = calculate_quality_score(dimensions)
# {'overall_score': 97.15, 'grade': 'A', ...}
```

### 品質スコアの閾値

| グレード | スコア | 対応 |
|---------|--------|------|
| A | 95〜100 | 正常。追加対応不要 |
| B | 90〜94 | 許容範囲。トレンド監視 |
| C | 80〜89 | 警告。原因調査と改善計画を策定 |
| D | 70〜79 | 要対応。データ利用者に通知 |
| F | 70未満 | 重大。パイプラインを停止して即座に対応 |

---

## チェックリスト

### データ品質ルール設計時
- [ ] 6次元（完全性, 正確性, 一貫性, 適時性, 一意性, 有効性）すべてを検討したか
- [ ] 各次元の検証ルールを具体的に定義したか
- [ ] 品質スコアの閾値とアクション（A〜F）を定義したか
- [ ] Great Expectations のスイートを構成したか

### データ品質検証実行時
- [ ] 検証結果をログに構造化形式で出力しているか
- [ ] 品質スコアがダッシュボードで可視化されているか
- [ ] 閾値を下回った場合のアラートが設定されているか
- [ ] エラーデータを削除せず分離しているか
- [ ] 品質トレンド（日次推移）を記録しているか
