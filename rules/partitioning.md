# パーティション戦略

データの物理配置とファイルフォーマットの選択に関するルール。クエリ性能、ストレージ効率、運用性の最適なバランスを実現する。

---

## 1. パーティションキーの選定基準

### 選定の原則

| 基準 | 説明 | 例 |
|------|------|-----|
| クエリパターン | WHERE 句で最も頻繁に使用されるカラム | `process_date`, `region` |
| カーディナリティ | 適度な数のパーティション（数百〜数千） | 日付（365/年）、地域（10〜50） |
| データ分布 | パーティション間のデータ量が均等 | 日付（日次データ量が安定） |
| 時系列 | データの時間的な増分管理 | `year/month/day` |

### パーティションキーの選定フロー

```
クエリパターンを分析
  ├─ 時系列データ？
  │     YES → 日付パーティション（year/month/day or process_date）
  │     NO ↓
  ├─ 地理的なフィルタが多い？
  │     YES → リージョン/国パーティション
  │     NO ↓
  ├─ カテゴリ別のアクセスが多い？
  │     YES → カテゴリパーティション
  │     NO ↓
  └─ パーティションなし（小規模テーブル）
```

### 複合パーティションの設計

```python
# 階層パーティション: year/month/day
df.write \
    .format("delta") \
    .partitionBy("year", "month") \
    .mode("overwrite") \
    .saveAsTable("silver.events")

# 注意: パーティションカラムの組み合わせが多すぎないこと
# ❌ BAD: partitionBy("year", "month", "day", "hour", "region", "category")
# → パーティション数が爆発し、Small File Problem が発生
# ✅ GOOD: partitionBy("process_date") または partitionBy("year", "month")
```

---

## 2. 適切なパーティション数の計算方法

### ファイルサイズの目安

| ファイルサイズ | 評価 | 対応 |
|--------------|------|------|
| < 1MB | 過小（Small File Problem） | Compaction が必要 |
| 1MB〜32MB | 小さい | 改善の余地あり |
| 32MB〜128MB | 適切（下限） | 許容範囲 |
| 128MB〜512MB | 最適 | 推奨ゾーン |
| 512MB〜1GB | 適切（上限） | 許容範囲 |
| > 1GB | 過大 | パーティション分割を検討 |

### パーティション数の計算

```python
def calculate_optimal_partitions(
    total_data_size_gb: float,
    target_file_size_mb: int = 256
) -> int:
    """最適なパーティション数を計算"""
    total_size_mb = total_data_size_gb * 1024
    optimal_partitions = int(total_size_mb / target_file_size_mb)
    return max(1, optimal_partitions)

# 例: 100GB のデータ、目標ファイルサイズ 256MB
partitions = calculate_optimal_partitions(100, 256)
# → 400 パーティション

# Spark での適用
df.repartition(partitions).write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver.large_table")
```

### Spark の並列度設定

```python
# Spark のシャッフルパーティション数（デフォルト: 200）
spark.conf.set("spark.sql.shuffle.partitions", "auto")  # Spark 3.x の AQE で自動調整

# AQE (Adaptive Query Execution) の有効化
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionSize", "64MB")
```

---

## 3. 日付パーティション vs ハッシュパーティション

| 観点 | 日付パーティション | ハッシュパーティション |
|------|-------------------|---------------------|
| データ分布 | 不均一の可能性あり | 均一 |
| 範囲クエリ | 高効率（パーティションプルーニング） | 非効率 |
| 等値クエリ | 効率的 | 最高効率 |
| 増分ロード | 容易（日付単位で上書き） | 困難 |
| データ削除 | 容易（パーティション単位） | 困難 |
| 可読性 | 高い（ディレクトリが日付） | 低い（ハッシュ値） |
| 主な用途 | 時系列データ、ログ | 高カーディナリティキーの均等分散 |

### 日付パーティションの実装

```python
from pyspark.sql.functions import year, month, dayofmonth, col

# 日付パーティションの作成
df_with_partition = (
    df
    .withColumn("year", year(col("event_timestamp")))
    .withColumn("month", month(col("event_timestamp")))
)

df_with_partition.write \
    .format("delta") \
    .partitionBy("year", "month") \
    .mode("overwrite") \
    .saveAsTable("silver.events")
```

### ハッシュパーティションの実装

```python
from pyspark.sql.functions import hash as spark_hash, abs as spark_abs

NUM_BUCKETS = 64

# ハッシュバケットの作成
df_bucketed = df.withColumn(
    "bucket_id",
    spark_abs(spark_hash(col("user_id"))) % NUM_BUCKETS
)

# バケットによるパーティション
df_bucketed.write \
    .format("delta") \
    .partitionBy("bucket_id") \
    .mode("overwrite") \
    .saveAsTable("silver.user_events")
```

---

## 4. ファイルフォーマット比較

| 観点 | Parquet | Delta Lake | Apache Iceberg | ORC | Avro |
|------|---------|------------|----------------|-----|------|
| 形式 | 列指向 | 列指向 (Parquet+) | 列指向 (Parquet/ORC) | 列指向 | 行指向 |
| ACID | 不可 | 対応 | 対応 | 不可 | 不可 |
| タイムトラベル | 不可 | 対応 | 対応 | 不可 | 不可 |
| スキーマ進化 | 限定的 | 対応 | 対応 | 限定的 | 対応 |
| 圧縮率 | 高い | 高い | 高い | 高い | 中程度 |
| 読み取り性能 | 高速 | 高速 | 高速 | 高速 | 遅い |
| 書き込み性能 | 高速 | 高速 | 高速 | 中程度 | 高速 |
| ストリーミング | 限定的 | 対応 | 対応 | 不可 | 対応 |
| 小ファイル最適化 | 手動 | Auto-optimize | 自動 Compaction | 手動 | 不要 |
| エコシステム | 広い | Databricks 中心 | オープン | Hive 中心 | Kafka 連携 |
| 推奨用途 | 分析 (読み取り多) | Lakehouse | マルチエンジン Lakehouse | Hive 環境 | メッセージング |

### フォーマット選択の判断フロー

```
ユースケースを確認
  ├─ ACID トランザクションが必要？
  │     YES ↓
  │     ├─ Databricks 環境？
  │     │     YES → Delta Lake
  │     │     NO → Apache Iceberg
  │     NO ↓
  ├─ Kafka / メッセージング連携？
  │     YES → Avro
  │     NO ↓
  ├─ Hive 環境で最適化が必要？
  │     YES → ORC
  │     NO → Parquet（デフォルト選択）
```

---

## 5. Small File Problem と対策

### 問題

大量の小さなファイル（< 1MB）が生成されると:
- メタデータ操作のオーバーヘッドが増大
- クエリ計画に時間がかかる
- ストレージのファイルリスト取得が遅延

### 原因

| 原因 | 状況 |
|------|------|
| ストリーミング | マイクロバッチごとに小ファイルが生成される |
| 過度なパーティション | パーティション数が多すぎてデータが分散 |
| 頻繁な append | 小さなバッチの append が繰り返される |
| 並列度の過設定 | repartition の値が大きすぎる |

### 対策: Compaction（ファイル統合）

```python
# Delta Lake: OPTIMIZE コマンド
spark.sql("""
    OPTIMIZE silver.events
    WHERE process_date >= '2026-03-01'
""")

# ファイルサイズのターゲット設定
spark.conf.set("spark.databricks.delta.optimize.maxFileSize", "268435456")  # 256MB
```

### 対策: Auto-optimize（Delta Lake）

```sql
-- テーブル作成時に Auto-optimize を有効化
ALTER TABLE silver.events
SET TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);
```

```python
# SparkSession レベルで有効化
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
```

### 対策: 書き込み時の coalesce

```python
# 書き込み前にパーティション数を減らす
df.coalesce(4).write \
    .format("delta") \
    .mode("append") \
    .saveAsTable("silver.events")

# repartition vs coalesce
# repartition: シャッフルあり、均等分散、パーティション増減可
# coalesce:    シャッフルなし、パーティション減少のみ、高速
```

---

## 6. Z-order / Liquid Clustering

### Z-order

複数カラムの値に基づいてデータをコロケーション（近い値を近いファイルに配置）する。フィルタ性能が大幅に向上する。

```sql
-- Z-order の適用（OPTIMIZE と同時に実行）
OPTIMIZE silver.orders
ZORDER BY (customer_id, order_date);
```

```python
# Z-order の適用基準
# ✅ 推奨: WHERE 句で頻繁にフィルタされるカラム（2〜4カラム）
# ❌ 非推奨: カーディナリティが非常に低いカラム（boolean 等）
# ❌ 非推奨: 5カラム以上の Z-order（効果が薄れる）
```

### Liquid Clustering（Delta Lake 3.x）

Z-order の進化版。書き込み時に自動的にクラスタリングが適用される。

```sql
-- Liquid Clustering の設定
CREATE TABLE silver.orders (
    order_id STRING,
    customer_id STRING,
    order_date DATE,
    amount DECIMAL(10,2)
) USING DELTA
CLUSTER BY (customer_id, order_date);

-- クラスタリングカラムの変更（オンラインで変更可能）
ALTER TABLE silver.orders
CLUSTER BY (region, order_date);
```

### Z-order vs Liquid Clustering

| 観点 | Z-order | Liquid Clustering |
|------|---------|-------------------|
| 適用タイミング | OPTIMIZE 時（手動/スケジュール） | 書き込み時に自動 |
| パーティションとの併用 | パーティション内で Z-order | パーティション不要（置換） |
| カラム変更 | 再 OPTIMIZE が必要 | ALTER で即座に変更 |
| 増分データ | 新ファイルは未 Z-order | 自動適用 |
| 可用性 | Delta Lake 1.x+ | Delta Lake 3.x+ |

---

## チェックリスト

### パーティション設計時
- [ ] クエリパターンを分析し、最適なパーティションキーを選定したか
- [ ] パーティション数が適切か（数百〜数千、ファイルサイズ 128MB〜1GB 目安）
- [ ] 複合パーティションのカーディナリティが爆発しないか確認したか
- [ ] 日付パーティション vs ハッシュパーティションの判断を行ったか

### ファイルフォーマット選択時
- [ ] ACID 要件を確認し、適切なフォーマットを選択したか
- [ ] タイムトラベルの要否を確認したか
- [ ] エコシステムとの互換性を確認したか

### Small File 対策
- [ ] Small File Problem のリスクを評価したか
- [ ] Delta Lake の Auto-optimize を有効化したか
- [ ] 定期的な OPTIMIZE / Compaction のスケジュールを設定したか
- [ ] ストリーミングパイプラインで coalesce を設定したか

### データコロケーション
- [ ] フィルタ頻度の高いカラムに Z-order / Liquid Clustering を設定したか
- [ ] Z-order のカラム数が 4 以下か
- [ ] クラスタリングの効果をクエリ性能で検証したか
