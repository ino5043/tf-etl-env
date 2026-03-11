# ETL/ELT パイプラインパターン

ETL/ELT パイプラインの設計パターンに関するルール。データ取り込み方式の選択からパイプラインの合成まで、再現性と信頼性のある設計を行うための指針。

---

## 1. ETL vs ELT の選択基準

データ統合の基本アーキテクチャとして、ETL と ELT のどちらを採用するかはシステム構成と要件に応じて判断する。

| 観点 | ETL | ELT |
|------|-----|-----|
| 変換タイミング | ロード前に中間層で変換 | ロード後にターゲット側で変換 |
| 計算資源 | ETL サーバー（Spark, Airflow Worker） | DWH/Lakehouse の計算資源 |
| 適合するデータ量 | 中規模（〜数TB） | 大規模（数TB〜PB） |
| 変換の柔軟性 | Python/Spark で自由に記述 | SQL ベースが中心 |
| レイテンシ | 変換処理分の遅延あり | 生データを即座にロード可能 |
| データレイクとの親和性 | 中程度 | 高い（Bronze 層にそのまま格納） |
| コスト | 中間サーバーの常時稼働コスト | DWH 課金（クエリ単位 or 容量単位） |
| ガバナンス | 変換前データが残らない場合あり | 生データが保持されるため監査容易 |

### 選択の判断フロー

```
データソースの特性を確認
  ├─ 構造化データ + DWH 中心？
  │     YES → ELT（SQL 変換で十分）
  │     NO ↓
  ├─ 非構造化 / 半構造化データ？
  │     YES → ETL（Python/Spark で前処理が必要）
  │     NO ↓
  ├─ リアルタイム要件あり？
  │     YES → ELT + Streaming（即座にロード → 変換）
  │     NO → バッチ ETL
```

---

## 2. CDC（Change Data Capture）パターン

ソースシステムの変更分のみを効率的に取り込むための3つの主要パターン。

### 2.1 ログベース CDC

データベースのトランザクションログ（WAL, Binlog）から変更を検出する。最も信頼性が高い。

```python
# Debezium + Kafka を使ったログベース CDC の消費例
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, schema_of_json

spark = SparkSession.builder.appName("cdc-consumer").getOrCreate()

# Kafka からの CDC イベントを読み取り
cdc_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "dbserver1.inventory.orders")
    .option("startingOffsets", "latest")
    .load()
)

# CDC イベントのパース
parsed = cdc_stream.select(
    from_json(col("value").cast("string"), cdc_schema).alias("data")
).select(
    col("data.after.*"),
    col("data.op").alias("_cdc_operation"),  # c=create, u=update, d=delete
    col("data.ts_ms").alias("_cdc_timestamp")
)
```

### 2.2 タイムスタンプベース CDC

`updated_at` カラムを利用して前回以降の変更を検出する。実装が容易だが、物理削除を検出できない。

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, max as spark_max
from datetime import datetime

def extract_incremental_by_timestamp(
    spark: SparkSession,
    source_table: str,
    target_table: str,
    timestamp_column: str = "updated_at"
) -> None:
    """タイムスタンプベースの増分抽出"""

    # 前回の最大タイムスタンプを取得（ハイウォーターマーク）
    try:
        hwm_df = spark.read.table(target_table)
        last_hwm = hwm_df.agg(
            spark_max(col(timestamp_column))
        ).collect()[0][0]
    except Exception:
        last_hwm = datetime(1970, 1, 1)

    # ハイウォーターマーク以降のレコードを抽出
    incremental_df = (
        spark.read.table(source_table)
        .filter(col(timestamp_column) > lit(last_hwm))
    )

    record_count = incremental_df.count()
    print(f"Extracted {record_count} records since {last_hwm}")

    return incremental_df
```

### 2.3 スナップショット比較 CDC

前回スナップショットと今回スナップショットを比較して差分を検出する。すべての変更タイプを検出可能だが、計算コストが高い。

```python
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, coalesce, when

def detect_changes_by_snapshot(
    current_df: DataFrame,
    previous_df: DataFrame,
    key_columns: list[str],
    compare_columns: list[str]
) -> dict[str, DataFrame]:
    """スナップショット比較による変更検出"""

    join_condition = [
        current_df[k] == previous_df[k] for k in key_columns
    ]

    # INSERT: 今回にあって前回にない
    inserts = current_df.join(
        previous_df, join_condition, "left_anti"
    )

    # DELETE: 前回にあって今回にない
    deletes = previous_df.join(
        current_df, join_condition, "left_anti"
    )

    # UPDATE: 両方にあるがカラム値が異なる
    matched = current_df.alias("curr").join(
        previous_df.alias("prev"), join_condition, "inner"
    )

    change_condition = None
    for c in compare_columns:
        cond = col(f"curr.{c}") != col(f"prev.{c}")
        change_condition = cond if change_condition is None else (change_condition | cond)

    updates = matched.filter(change_condition).select("curr.*")

    return {
        "inserts": inserts,
        "updates": updates,
        "deletes": deletes,
        "stats": {
            "insert_count": inserts.count(),
            "update_count": updates.count(),
            "delete_count": deletes.count(),
        }
    }
```

### CDC パターン比較

| 観点 | ログベース | タイムスタンプ | スナップショット比較 |
|------|-----------|--------------|-------------------|
| 削除の検出 | 可能 | 不可（論理削除なら可） | 可能 |
| ソースへの影響 | 低い（ログ読み取り） | 低い | 高い（フルスキャン） |
| 実装難易度 | 高い（Debezium 等が必要） | 低い | 中程度 |
| 遅延 | ほぼリアルタイム | バッチ間隔に依存 | バッチ間隔に依存 |
| 正確性 | 最高（全変更を捕捉） | 中程度 | 高い |
| 前提条件 | DB のログアクセス権 | updated_at カラム | 前回スナップショット保持 |

---

## 3. 増分ロード vs フルロード

| 観点 | 増分ロード | フルロード |
|------|----------|----------|
| データ転送量 | 少ない（差分のみ） | 多い（全量） |
| 処理時間 | 短い | 長い |
| 実装の複雑性 | 高い（状態管理が必要） | 低い |
| データ整合性 | 実装に依存 | 常に整合 |
| 障害復旧 | ハイウォーターマーク管理 | 再実行で完全復旧 |

### 判断基準

```
データ量を確認
  ├─ テーブルサイズ < 100MB？
  │     YES → フルロード（シンプルさ優先）
  │     NO ↓
  ├─ 日次変更率 < 10%？
  │     YES → 増分ロード（効率優先）
  │     NO ↓
  ├─ 変更率 ≥ 10% かつサイズ < 10GB？
  │     YES → フルロード（整合性優先）
  │     NO → 増分ロード + 定期フルリフレッシュ
```

---

## 4. マイクロバッチパターン

ストリーミングとバッチの中間。短い間隔（1〜15分）でバッチ処理を繰り返す。

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("micro-batch").getOrCreate()

# Structured Streaming をマイクロバッチモードで実行
query = (
    spark.readStream
    .format("delta")
    .table("bronze.raw_events")
    .writeStream
    .format("delta")
    .outputMode("append")
    .trigger(processingTime="5 minutes")  # 5分間隔のマイクロバッチ
    .option("checkpointLocation", "/checkpoints/events_silver")
    .toTable("silver.processed_events")
)

query.awaitTermination()
```

---

## 5. パイプライン合成パターン

### 5.1 Chain（直列）

```python
def pipeline_chain(spark, source_table: str, target_table: str):
    """直列パイプライン: Extract → Transform → Load"""
    raw_df = extract(spark, source_table)
    cleaned_df = cleanse(raw_df)
    enriched_df = enrich(cleaned_df)
    load(enriched_df, target_table)
```

### 5.2 Fan-out（分岐）

1つのソースから複数のターゲットへ分配する。

```python
def pipeline_fan_out(spark, source_table: str):
    """Fan-out: 1つのソースを複数ターゲットに分配"""
    raw_df = extract(spark, source_table).cache()

    # 同じソースから異なるターゲットへ
    summary_df = aggregate_daily(raw_df)
    load(summary_df, "gold.daily_summary")

    anomaly_df = detect_anomalies(raw_df)
    load(anomaly_df, "gold.anomaly_alerts")

    archive_df = add_metadata(raw_df)
    load(archive_df, "silver.archived_events")

    raw_df.unpersist()
```

### 5.3 Fan-in（統合）

複数のソースを統合して1つのターゲットへ書き込む。

```python
def pipeline_fan_in(spark, target_table: str):
    """Fan-in: 複数ソースを統合して1つのターゲットへ"""
    orders_df = extract(spark, "source.orders")
    customers_df = extract(spark, "source.customers")
    products_df = extract(spark, "source.products")

    # 統合
    enriched_orders = (
        orders_df
        .join(customers_df, "customer_id", "left")
        .join(products_df, "product_id", "left")
    )

    load(enriched_orders, target_table)
```

### パイプライン合成パターンの選択基準

| パターン | ユースケース | 注意点 |
|---------|------------|--------|
| Chain | 単純な段階的処理 | 途中失敗時の再開を考慮 |
| Fan-out | 1ソースから複数集計 | ソースの `.cache()` を忘れない |
| Fan-in | マスタ結合、統合テーブル | 結合キーの整合性を検証 |
| Diamond | Fan-out + Fan-in の組み合わせ | データの重複カウントに注意 |

---

## チェックリスト

### パイプライン設計時
- [ ] ETL vs ELT の選択理由を明確にしたか（ADR として記録推奨）
- [ ] CDC パターンを選定し、ソースシステムの前提条件を確認したか
- [ ] 増分ロード vs フルロードの判断基準に基づいて方式を選択したか
- [ ] パイプラインの合成パターン（Chain / Fan-out / Fan-in）を適切に選択したか
- [ ] マイクロバッチの間隔はデータ鮮度要件と計算コストのバランスが取れているか

### パイプライン実装時
- [ ] 冪等性が確保されているか（`idempotency-patterns.md` 参照）
- [ ] エラーハンドリングと処理統計の記録があるか
- [ ] チェックポイントによる再開が可能か
- [ ] ログに十分なコンテキスト情報が含まれているか
- [ ] パフォーマンステスト（データ量の増加に対するスケーラビリティ）を実施したか
