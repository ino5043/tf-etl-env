---
name: spark-tuning
description: Spark job tuning guide with configuration templates, diagnostic procedures, and optimization checklists.
---

# Spark Tuning

Spark ジョブのパフォーマンスチューニングガイド。設定テンプレート、診断手順、最適化チェックリストを提供する。

---

## Spark 設定テンプレート

### 基本設定（spark-defaults.conf）

```properties
# === Executor 設定 ===
spark.executor.instances=4
spark.executor.cores=4
spark.executor.memory=8g
spark.executor.memoryOverhead=2g

# === Driver 設定 ===
spark.driver.memory=4g
spark.driver.memoryOverhead=1g
spark.driver.maxResultSize=2g

# === 並列度 ===
spark.default.parallelism=200
spark.sql.shuffle.partitions=200

# === メモリ管理 ===
spark.memory.fraction=0.6
spark.memory.storageFraction=0.5

# === シリアライゼーション ===
spark.serializer=org.apache.spark.serializer.KryoSerializer
spark.kryoserializer.buffer.max=1024m

# === Adaptive Query Execution (AQE) ===
spark.sql.adaptive.enabled=true
spark.sql.adaptive.coalescePartitions.enabled=true
spark.sql.adaptive.skewJoin.enabled=true
spark.sql.adaptive.skewJoin.skewedPartitionFactor=5
spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes=256m

# === Dynamic Allocation ===
spark.dynamicAllocation.enabled=true
spark.dynamicAllocation.minExecutors=2
spark.dynamicAllocation.maxExecutors=20
spark.dynamicAllocation.executorIdleTimeout=60s

# === Speculation ===
spark.speculation=true
spark.speculation.multiplier=1.5
spark.speculation.quantile=0.9
```

### ワークロード別推奨設定

| ワークロード | executor.instances | executor.cores | executor.memory | shuffle.partitions |
|-------------|-------------------|---------------|-----------------|-------------------|
| 小規模バッチ (<1GB) | 2 | 2 | 4g | 50 |
| 中規模バッチ (1-100GB) | 4-8 | 4 | 8g | 200 |
| 大規模バッチ (100GB-1TB) | 10-20 | 4-5 | 16g | 500-1000 |
| ストリーミング | 4-8 | 2 | 4g | 100 |

---

## 診断手順

### 1. Spark UI の確認ポイント

| タブ | 確認項目 | 異常の兆候 |
|------|---------|-----------|
| Jobs | ジョブの実行時間 | 特定ジョブだけ極端に遅い |
| Stages | ステージの Task 数と時間 | Task 間の時間差が大きい（Skew） |
| Storage | キャッシュの使用状況 | メモリ不足でキャッシュが追い出される |
| Environment | 設定値 | デフォルト値のまま |
| Executors | GC 時間、Shuffle Read/Write | GC 時間がトータルの10%超 |
| SQL | 実行計画 | BroadcastHashJoin が期待通りか |

### 2. 実行計画の読み方

```python
# 論理実行計画の確認
df.explain(mode="simple")

# 物理実行計画の確認（推奨）
df.explain(mode="extended")

# コスト付き実行計画（Spark 3.x）
df.explain(mode="cost")

# フォーマット済み実行計画
df.explain(mode="formatted")
```

### 確認すべきオペレータ

| オペレータ | 意味 | 注意点 |
|-----------|------|--------|
| `BroadcastHashJoin` | 小テーブルをブロードキャスト | 期待通りか確認 |
| `SortMergeJoin` | ソート後にマージ結合 | 大テーブル同士で使用 |
| `ShuffledHashJoin` | シャッフル後にハッシュ結合 | AQE で自動選択 |
| `Exchange` | シャッフル発生 | 回数を最小化する |
| `Filter` | フィルタ適用 | Predicate Pushdown されているか |
| `Project` | カラム選択 | Column Pruning されているか |

---

## よくあるボトルネックと対処法

### 1. データスキュー（Data Skew）

**症状**: 一部の Task だけ極端に遅い

```python
# 対処法1: AQE のスキュー対応を有効化
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

# 対処法2: ソルトキーで分散
from pyspark.sql import functions as F

salt_num = 10
df_salted = df.withColumn(
    "salt", (F.rand() * salt_num).cast("int")
)
# salt を結合キーに追加して分散させる
```

### 2. シャッフルの過多

**症状**: Shuffle Read/Write が異常に大きい

```python
# 対処法1: ブロードキャスト結合の強制
from pyspark.sql import functions as F

df_result = df_large.join(
    F.broadcast(df_small),  # 小テーブルをブロードキャスト
    on="key_column",
)

# 対処法2: Bucket Join（事前パーティション）
df.write.bucketBy(64, "key_column").sortBy("key_column").saveAsTable("bucketed_table")
```

### 3. GC オーバーヘッド

**症状**: Executor の GC 時間が 10% 超

```properties
# 対処法: G1GC の使用とメモリ調整
spark.executor.extraJavaOptions=-XX:+UseG1GC -XX:G1HeapRegionSize=16m
spark.executor.memory=16g
spark.executor.memoryOverhead=4g
spark.memory.fraction=0.7
```

### 4. 小さいファイル問題（Small Files Problem）

**症状**: 大量の小さいファイルが生成される

```python
# 対処法1: coalesce で出力ファイル数を制御
df.coalesce(10).write.format("delta").save(path)

# 対処法2: repartition でパーティション数を制御
df.repartition(20).write.format("delta").save(path)

# 対処法3: Delta Lake の OPTIMIZE
spark.sql("OPTIMIZE delta.`/path/to/table`")
```

---

## Join 戦略の選択

| 戦略 | 条件 | メリット | デメリット |
|------|------|---------|-----------|
| Broadcast Hash Join | 片方が小さい (<10MB) | シャッフル不要、最速 | メモリ消費 |
| Sort Merge Join | 両方が大きい | 安定したパフォーマンス | ソート＋シャッフルが必要 |
| Bucket Join | 事前にバケット化済み | シャッフル不要 | 事前準備が必要 |
| Shuffle Hash Join | 片方がやや小さい | ソート不要 | メモリ消費 |

```python
# ブロードキャスト閾値の設定
spark.conf.set(
    "spark.sql.autoBroadcastJoinThreshold",
    10 * 1024 * 1024  # 10MB（デフォルト）
)
```

---

## パーティション最適化

### パーティション数の目安

| データ量 | 推奨パーティション数 | 1パーティションあたり |
|---------|--------------------|--------------------|
| < 1GB | 10-50 | 20-100MB |
| 1-10GB | 50-200 | 50-200MB |
| 10-100GB | 200-500 | 100-200MB |
| 100GB-1TB | 500-2000 | 128-256MB |

### 最適パーティション数の計算

```python
def calculate_optimal_partitions(
    data_size_bytes: int,
    target_partition_size_mb: int = 128,
) -> int:
    """最適なパーティション数を計算する"""
    target_size_bytes = target_partition_size_mb * 1024 * 1024
    optimal = max(1, data_size_bytes // target_size_bytes)
    return optimal
```

---

## 診断チェックリスト

### パフォーマンス診断

- [ ] Spark UI の Jobs タブで異常に遅いジョブがないか
- [ ] Stages タブで Task 間の処理時間にばらつきがないか（Skew）
- [ ] Executors タブで GC 時間が 10% を超えていないか
- [ ] Shuffle Read/Write のサイズが想定範囲か
- [ ] 実行計画で期待する Join 戦略が選択されているか
- [ ] Predicate Pushdown と Column Pruning が効いているか

### 設定チェック

- [ ] AQE（Adaptive Query Execution）が有効か
- [ ] shuffle.partitions がデータ量に見合っているか
- [ ] executor.memory と memoryOverhead が適切か
- [ ] ブロードキャスト閾値が適切か
- [ ] Dynamic Allocation が有効か（必要に応じて）

### 出力チェック

- [ ] 出力ファイル数が適切か（小さいファイル問題がないか）
- [ ] パーティションキーが適切か
- [ ] 圧縮コーデックが設定されているか（snappy, zstd）

---

## 注意事項

- チューニングはボトルネックの特定が最優先。推測でパラメータを変更しない
- AQE を有効にすると、多くの最適化が自動的に適用される（Spark 3.x 以降推奨）
- ブロードキャスト結合は Driver のメモリを消費するため、閾値の設定に注意
- パフォーマンス改善の効果は必ず計測値で確認する
