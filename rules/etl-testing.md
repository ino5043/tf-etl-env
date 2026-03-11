# ETL テスト戦略

ETL パイプラインの品質を保証するためのテスト戦略。テストピラミッドに基づき、Unit テストから E2E テストまでの各レベルのテスト方法を定義する。

---

## 1. ETL テストピラミッド

```
          ┌─────────┐
          │  E2E    │  少ない: パイプライン全体の結合テスト
         ─┤  Test   ├─
        ┌─┴─────────┴─┐
        │ Integration  │  中程度: パイプライン単位のテスト
       ─┤    Test      ├─
      ┌─┴──────────────┴─┐
      │  Data Quality    │  中程度: Great Expectations スイート
     ─┤     Test         ├─
    ┌─┴──────────────────┴─┐
    │      Unit Test       │  多い: 変換関数の個別テスト
    └──────────────────────┘
```

| テストレベル | テスト数 | 実行速度 | 対象 |
|------------|---------|---------|------|
| Unit Test | 多い (60%) | 高速 (秒) | 個別の変換関数 |
| Data Quality Test | 中程度 (20%) | 中速 (分) | データの品質ルール |
| Integration Test | 中程度 (15%) | 中速 (分) | パイプライン単位 |
| E2E Test | 少ない (5%) | 低速 (分〜時) | Bronze→Silver→Gold 全体 |

---

## 2. Unit Test: pytest + PySpark

### 2.1 SparkSession のフィクスチャ

```python
# conftest.py
import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    """テスト用の SparkSession（セッション全体で共有）"""
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("etl-unit-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse-test")
        .config("spark.driver.extraJavaOptions", "-Dderby.system.home=/tmp/derby-test")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .getOrCreate()
    )
    yield session
    session.stop()

@pytest.fixture(scope="function")
def sample_orders_df(spark):
    """テスト用の注文 DataFrame"""
    data = [
        ("ORD-001", "CUST-001", "2026-03-10", 100, 2.50, "CONFIRMED"),
        ("ORD-002", "CUST-002", "2026-03-10", 50, 10.00, "PENDING"),
        ("ORD-003", "CUST-001", "2026-03-11", 200, 5.00, "CONFIRMED"),
        ("ORD-004", None, "2026-03-11", 30, 3.00, "CANCELLED"),
        ("ORD-005", "CUST-003", None, 10, 1.00, "CONFIRMED"),
    ]

    schema = "order_id STRING, customer_id STRING, order_date_str STRING, quantity INT, unit_price DOUBLE, status STRING"
    return spark.createDataFrame(data, schema=schema)
```

### 2.2 DataFrame の比較方法

```python
# test_helpers.py
from pyspark.sql import DataFrame

def assert_dataframes_equal(
    actual: DataFrame,
    expected: DataFrame,
    order_by: list[str] | None = None
) -> None:
    """2つの DataFrame が等しいことを検証"""
    if order_by:
        actual = actual.orderBy(order_by)
        expected = expected.orderBy(order_by)

    # スキーマの比較
    assert actual.schema == expected.schema, (
        f"Schema mismatch:\n"
        f"  Actual:   {actual.schema.simpleString()}\n"
        f"  Expected: {expected.schema.simpleString()}"
    )

    # データの比較
    actual_rows = [row.asDict() for row in actual.collect()]
    expected_rows = [row.asDict() for row in expected.collect()]

    assert len(actual_rows) == len(expected_rows), (
        f"Row count mismatch: actual={len(actual_rows)}, expected={len(expected_rows)}"
    )

    for i, (a, e) in enumerate(zip(actual_rows, expected_rows)):
        assert a == e, f"Row {i} mismatch:\n  Actual:   {a}\n  Expected: {e}"


def assert_row_count(df: DataFrame, expected_count: int) -> None:
    """DataFrame の行数を検証"""
    actual_count = df.count()
    assert actual_count == expected_count, (
        f"Row count: actual={actual_count}, expected={expected_count}"
    )
```

### 2.3 変換関数のテスト例

```python
# transforms.py - テスト対象の変換関数
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_date, trim, upper, when

def clean_orders(df: DataFrame) -> DataFrame:
    """注文データのクレンジング"""
    return (
        df
        .filter(col("order_id").isNotNull())
        .filter(col("customer_id").isNotNull())
        .withColumn("order_date", to_date(col("order_date_str"), "yyyy-MM-dd"))
        .withColumn("status", upper(trim(col("status"))))
        .withColumn("total_amount", col("unit_price") * col("quantity"))
        .drop("order_date_str")
    )

def categorize_order_size(df: DataFrame) -> DataFrame:
    """注文サイズの分類"""
    return df.withColumn(
        "order_size",
        when(col("total_amount") >= 1000, "LARGE")
        .when(col("total_amount") >= 100, "MEDIUM")
        .otherwise("SMALL")
    )
```

```python
# test_transforms.py
import pytest
from transforms import clean_orders, categorize_order_size
from test_helpers import assert_row_count

class TestCleanOrders:
    def test_removes_null_order_id(self, spark, sample_orders_df):
        """order_id が NULL のレコードが除外されること"""
        result = clean_orders(sample_orders_df)
        null_order_ids = result.filter(col("order_id").isNull()).count()
        assert null_order_ids == 0

    def test_removes_null_customer_id(self, spark, sample_orders_df):
        """customer_id が NULL のレコードが除外されること"""
        result = clean_orders(sample_orders_df)
        # ORD-004 (customer_id=NULL) が除外される
        assert_row_count(result, 4)  # 5件中、customer_id=NULL の1件が除外

    def test_date_conversion(self, spark, sample_orders_df):
        """order_date_str が DATE 型に変換されること"""
        result = clean_orders(sample_orders_df)
        assert result.schema["order_date"].dataType.simpleString() == "date"

    def test_status_standardization(self, spark, sample_orders_df):
        """status が大文字に標準化されること"""
        result = clean_orders(sample_orders_df)
        statuses = [row.status for row in result.select("status").distinct().collect()]
        for status in statuses:
            assert status == status.upper()

    def test_total_amount_calculation(self, spark, sample_orders_df):
        """total_amount が unit_price * quantity で計算されること"""
        result = clean_orders(sample_orders_df)
        row = result.filter(col("order_id") == "ORD-001").collect()[0]
        assert row.total_amount == 250.0  # 100 * 2.50

    def test_original_date_column_dropped(self, spark, sample_orders_df):
        """order_date_str カラムが削除されること"""
        result = clean_orders(sample_orders_df)
        assert "order_date_str" not in result.columns


class TestCategorizeOrderSize:
    def test_large_order(self, spark):
        data = [("ORD-001", 1500.0)]
        df = spark.createDataFrame(data, "order_id STRING, total_amount DOUBLE")
        result = categorize_order_size(df)
        assert result.collect()[0].order_size == "LARGE"

    def test_medium_order(self, spark):
        data = [("ORD-002", 500.0)]
        df = spark.createDataFrame(data, "order_id STRING, total_amount DOUBLE")
        result = categorize_order_size(df)
        assert result.collect()[0].order_size == "MEDIUM"

    def test_small_order(self, spark):
        data = [("ORD-003", 50.0)]
        df = spark.createDataFrame(data, "order_id STRING, total_amount DOUBLE")
        result = categorize_order_size(df)
        assert result.collect()[0].order_size == "SMALL"

    def test_boundary_1000(self, spark):
        data = [("ORD-004", 1000.0)]
        df = spark.createDataFrame(data, "order_id STRING, total_amount DOUBLE")
        result = categorize_order_size(df)
        assert result.collect()[0].order_size == "LARGE"

    def test_boundary_100(self, spark):
        data = [("ORD-005", 100.0)]
        df = spark.createDataFrame(data, "order_id STRING, total_amount DOUBLE")
        result = categorize_order_size(df)
        assert result.collect()[0].order_size == "MEDIUM"
```

---

## 3. Data Quality Test: Great Expectations

### スイート構成

```python
# tests/data_quality/test_orders_quality.py
import great_expectations as gx

def test_orders_silver_quality(spark):
    """Silver 注文テーブルの品質スイート"""
    context = gx.get_context()

    # Expectation Suite の定義
    suite = context.suites.add(
        gx.ExpectationSuite(name="orders_silver_suite")
    )

    # 完全性
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(column="order_id")
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(column="customer_id")
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(column="order_date")
    )

    # 一意性
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeUnique(column="order_id")
    )

    # 正確性
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="quantity", min_value=1, max_value=100000
        )
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="unit_price", min_value=0.01, max_value=1000000
        )
    )

    # 有効性
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="status",
            value_set=["PENDING", "CONFIRMED", "SHIPPED", "DELIVERED", "CANCELLED"]
        )
    )

    # レコード件数
    suite.add_expectation(
        gx.expectations.ExpectTableRowCountToBeBetween(
            min_value=1, max_value=10000000
        )
    )

    # 実行
    validation_result = context.run_checkpoint(
        checkpoint_name="orders_silver_checkpoint"
    )
    assert validation_result.success, "Data quality validation failed"
```

---

## 4. Integration Test: パイプライン単位

```python
# tests/integration/test_orders_pipeline.py
import pytest
from pipeline.orders import run_orders_pipeline

class TestOrdersPipeline:
    @pytest.fixture
    def setup_bronze_data(self, spark, tmp_path):
        """テスト用 Bronze データの作成"""
        test_data = [
            ("ORD-001", "CUST-001", "2026-03-10", 10, 100.0, "confirmed", "batch_001"),
            ("ORD-002", "CUST-002", "2026-03-10", 5, 200.0, "pending", "batch_001"),
            ("ORD-001", "CUST-001", "2026-03-10", 10, 100.0, "confirmed", "batch_001"),  # 重複
        ]
        schema = "order_id STRING, customer_id STRING, order_date_str STRING, quantity INT, unit_price DOUBLE, status STRING, _batch_id STRING"
        df = spark.createDataFrame(test_data, schema=schema)

        table_path = str(tmp_path / "bronze_orders")
        df.write.format("delta").save(table_path)
        return table_path

    def test_pipeline_produces_correct_count(self, spark, setup_bronze_data, tmp_path):
        """パイプラインが重複排除後の正しい件数を出力すること"""
        silver_path = str(tmp_path / "silver_orders")

        run_orders_pipeline(
            spark=spark,
            bronze_path=setup_bronze_data,
            silver_path=silver_path,
            process_date="2026-03-10"
        )

        result = spark.read.format("delta").load(silver_path)
        assert result.count() == 2  # 3件中、重複1件が除外

    def test_pipeline_idempotency(self, spark, setup_bronze_data, tmp_path):
        """パイプラインの冪等性: 2回実行しても結果が同じこと"""
        silver_path = str(tmp_path / "silver_orders")

        # 1回目
        run_orders_pipeline(spark, setup_bronze_data, silver_path, "2026-03-10")
        count_1 = spark.read.format("delta").load(silver_path).count()

        # 2回目（同一データで再実行）
        run_orders_pipeline(spark, setup_bronze_data, silver_path, "2026-03-10")
        count_2 = spark.read.format("delta").load(silver_path).count()

        assert count_1 == count_2, (
            f"Idempotency violation: run1={count_1}, run2={count_2}"
        )

    def test_pipeline_data_quality(self, spark, setup_bronze_data, tmp_path):
        """パイプライン出力のデータ品質"""
        silver_path = str(tmp_path / "silver_orders")
        run_orders_pipeline(spark, setup_bronze_data, silver_path, "2026-03-10")

        result = spark.read.format("delta").load(silver_path)

        # NULL チェック
        assert result.filter(col("order_id").isNull()).count() == 0
        assert result.filter(col("customer_id").isNull()).count() == 0

        # 型チェック
        assert result.schema["order_date"].dataType.simpleString() == "date"
        assert result.schema["total_amount"].dataType.simpleString() == "double"
```

---

## 5. Performance Test

```python
# tests/performance/test_pipeline_scalability.py
import time
import pytest

class TestPipelinePerformance:
    @pytest.fixture
    def large_dataset(self, spark):
        """大規模テストデータの生成"""
        from pyspark.sql.functions import expr

        df = (
            spark.range(0, 1000000)
            .withColumn("order_id", expr("concat('ORD-', lpad(cast(id as string), 10, '0'))"))
            .withColumn("customer_id", expr("concat('CUST-', cast(id % 10000 as string))"))
            .withColumn("order_date", expr("date_add('2026-01-01', cast(id % 365 as int))"))
            .withColumn("quantity", expr("cast(rand() * 100 + 1 as int)"))
            .withColumn("unit_price", expr("round(rand() * 1000, 2)"))
            .withColumn("status", expr("""
                CASE cast(rand() * 4 as int)
                    WHEN 0 THEN 'PENDING'
                    WHEN 1 THEN 'CONFIRMED'
                    WHEN 2 THEN 'SHIPPED'
                    ELSE 'DELIVERED'
                END
            """))
        )
        return df

    def test_transform_performance_1m_records(self, spark, large_dataset, tmp_path):
        """100万件の変換処理が60秒以内に完了すること"""
        from transforms import clean_orders

        start_time = time.time()
        result = clean_orders(large_dataset)
        result.write.format("delta").save(str(tmp_path / "perf_test"))
        elapsed = time.time() - start_time

        assert elapsed < 60, f"Performance threshold exceeded: {elapsed:.1f}s > 60s"
        print(f"Processed {large_dataset.count()} records in {elapsed:.1f}s")
```

---

## 6. テストデータ管理

### Faker を使ったテストデータ生成

```python
from faker import Faker
from pyspark.sql import SparkSession
import random

fake = Faker("ja_JP")
Faker.seed(42)  # 固定シード（再現性のため）
random.seed(42)

def generate_test_orders(spark: SparkSession, count: int):
    """テスト用注文データの生成"""
    data = []
    for i in range(count):
        data.append({
            "order_id": f"ORD-{i:06d}",
            "customer_id": f"CUST-{random.randint(1, count // 10):05d}",
            "customer_name": fake.name(),
            "email": fake.email(),
            "order_date": fake.date_between(
                start_date="-90d", end_date="today"
            ).isoformat(),
            "quantity": random.randint(1, 100),
            "unit_price": round(random.uniform(100, 50000), 2),
            "status": random.choice(
                ["PENDING", "CONFIRMED", "SHIPPED", "DELIVERED", "CANCELLED"]
            ),
        })

    return spark.createDataFrame(data)
```

### サンプリングによるテストデータ

```python
def create_test_sample(
    spark: SparkSession,
    source_table: str,
    sample_fraction: float = 0.01,
    seed: int = 42
):
    """本番データからサンプリングしてテストデータを作成"""
    return (
        spark.read.table(source_table)
        .sample(fraction=sample_fraction, seed=seed)
    )

# 使用例: 本番100万件の1%（1万件）をサンプリング
test_df = create_test_sample(spark, "silver.orders", sample_fraction=0.01)
```

---

## 7. pytest 設定

```toml
# pyproject.toml
[tool.pytest.ini_options]
testpaths = ["tests"]
python_files = ["test_*.py"]
python_classes = ["Test*"]
python_functions = ["test_*"]
addopts = [
    "--junitxml=test-results/junit.xml",
    "--html=test-results/report.html",
    "--self-contained-html",
    "-v",
    "--tb=short",
    "-x",  # 最初の失敗で停止
]
markers = [
    "unit: Unit tests (fast)",
    "integration: Integration tests (medium)",
    "e2e: End-to-end tests (slow)",
    "performance: Performance tests",
]

[tool.pytest.ini_options.filterwarnings]
ignore = ["ignore::DeprecationWarning"]
```

### テスト実行コマンド

```bash
# Unit テストのみ
pytest -m unit

# Integration テストのみ
pytest -m integration

# 全テスト（カバレッジ付き）
pytest --cov=pipeline --cov-report=html:test-results/coverage-html --cov-report=xml:test-results/coverage.xml

# Performance テスト
pytest -m performance --timeout=300
```

---

## チェックリスト

### テスト設計時
- [ ] テストピラミッドの比率（Unit 60%, DQ 20%, Integration 15%, E2E 5%）を意識しているか
- [ ] SparkSession のフィクスチャをセッションスコープで定義したか
- [ ] テストデータは固定シードで再現可能か

### Unit テスト
- [ ] 各変換関数に対してテストを書いたか
- [ ] 正常系・異常系・境界値のテストケースがあるか
- [ ] DataFrame の比較ヘルパーを活用しているか

### Integration テスト
- [ ] パイプライン単位のテストがあるか
- [ ] 冪等性テスト（2回実行で同一結果）を実施したか
- [ ] データ品質テスト（Great Expectations）を組み込んだか

### CI/CD 連携
- [ ] JUnit XML レポートが出力される設定か
- [ ] カバレッジレポートが生成される設定か
- [ ] テスト結果を Artifact として保存する CI 設定があるか
- [ ] テストカバレッジが 80% 以上か
