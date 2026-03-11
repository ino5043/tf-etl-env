# 冪等性パターン

ETL/ELT パイプラインにおける冪等性を確保するためのルール。同じ処理を複数回実行しても結果が変わらないことを保証し、障害復旧と運用の安全性を高める。

---

## 基本原則

**冪等性の定義**: 同じ入力で同じ処理を N 回実行しても、1 回実行した結果と同じになること。

```
f(x) = f(f(x)) = f(f(f(x))) = ...
```

ETL において冪等性がないと、以下の問題が発生する:
- 障害後の再実行でデータが重複する
- リトライ時に二重カウント・二重課金が起きる
- 手動復旧の手順が複雑になる

---

## 1. パーティション上書きパターン（最も推奨）

日付パーティション単位でデータを完全に上書きする。最もシンプルで信頼性が高い。

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

def load_daily_partition(
    spark: SparkSession,
    source_df,
    target_table: str,
    process_date: str
):
    """パーティション上書きによる冪等なロード"""
    # 処理対象日のデータのみ抽出
    daily_df = source_df.filter(col("process_date") == lit(process_date))

    # 対象パーティションのみ上書き（他のパーティションは影響なし）
    daily_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("replaceWhere", f"process_date = '{process_date}'") \
        .saveAsTable(target_table)

    record_count = daily_df.count()
    print(f"Loaded {record_count} records for {process_date} (idempotent overwrite)")
```

```sql
-- SQL での実装
-- Step 1: 対象パーティションを削除
DELETE FROM silver.daily_orders
WHERE process_date = '2026-03-10';

-- Step 2: 新しいデータを INSERT
INSERT INTO silver.daily_orders
SELECT *, '2026-03-10' AS process_date
FROM staging.orders
WHERE order_date = '2026-03-10';
```

### 適用条件

| 条件 | 評価 |
|------|------|
| パーティションキーが明確 | 必須（日付が一般的） |
| パーティション内のデータが完全に再構築可能 | 必須 |
| パーティション単位での全量再ロードが許容されるデータ量 | 1パーティション < 数GB が目安 |

---

## 2. MERGE/UPSERT パターン

主キーに基づいてレコード単位で INSERT or UPDATE を行う。行レベルの精密な制御が可能。

```python
from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp

def upsert_with_merge(
    spark: SparkSession,
    source_df,
    target_table: str,
    key_columns: list[str],
    update_columns: list[str]
):
    """MERGE による冪等な UPSERT"""
    target = DeltaTable.forName(spark, target_table)

    # マージ条件
    merge_condition = " AND ".join([
        f"target.{k} = source.{k}" for k in key_columns
    ])

    # UPDATE 対象カラム
    update_set = {c: f"source.{c}" for c in update_columns}
    update_set["updated_at"] = "current_timestamp()"

    # INSERT 対象カラム（全カラム）
    all_columns = key_columns + update_columns
    insert_values = {c: f"source.{c}" for c in all_columns}
    insert_values["created_at"] = "current_timestamp()"
    insert_values["updated_at"] = "current_timestamp()"

    target.alias("target").merge(
        source_df.alias("source"),
        merge_condition
    ).whenMatchedUpdate(
        set=update_set
    ).whenNotMatchedInsert(
        values=insert_values
    ).execute()
```

```sql
-- SQL での MERGE
MERGE INTO silver.products AS target
USING staging.products AS source
ON target.product_id = source.product_id
WHEN MATCHED THEN UPDATE SET
    target.product_name = source.product_name,
    target.price = source.price,
    target.category = source.category,
    target.updated_at = current_timestamp()
WHEN NOT MATCHED THEN INSERT (
    product_id, product_name, price, category, created_at, updated_at
) VALUES (
    source.product_id, source.product_name, source.price,
    source.category, current_timestamp(), current_timestamp()
);
```

### DELETE も含む MERGE

```sql
-- ソースに存在しないレコードを論理削除する MERGE
MERGE INTO silver.products AS target
USING staging.products AS source
ON target.product_id = source.product_id
WHEN MATCHED THEN UPDATE SET
    target.product_name = source.product_name,
    target.price = source.price,
    target.is_deleted = false,
    target.updated_at = current_timestamp()
WHEN NOT MATCHED BY TARGET THEN INSERT (
    product_id, product_name, price, is_deleted, created_at, updated_at
) VALUES (
    source.product_id, source.product_name, source.price,
    false, current_timestamp(), current_timestamp()
)
WHEN NOT MATCHED BY SOURCE AND target.is_deleted = false THEN UPDATE SET
    target.is_deleted = true,
    target.updated_at = current_timestamp();
```

---

## 3. チェックポイント + リスタートパターン

長時間実行するパイプラインで、処理済みの状態を記録し、障害時に途中から再開する。

```python
import json
from pathlib import Path
from dataclasses import dataclass, asdict
from datetime import datetime

@dataclass
class PipelineCheckpoint:
    pipeline_name: str
    last_processed_id: str
    last_processed_date: str
    records_processed: int
    status: str  # "in_progress" | "completed"
    updated_at: str

class CheckpointManager:
    def __init__(self, checkpoint_dir: str):
        self.checkpoint_dir = Path(checkpoint_dir)
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)

    def save(self, checkpoint: PipelineCheckpoint) -> None:
        path = self.checkpoint_dir / f"{checkpoint.pipeline_name}.json"
        with open(path, "w") as f:
            json.dump(asdict(checkpoint), f, indent=2)

    def load(self, pipeline_name: str) -> PipelineCheckpoint | None:
        path = self.checkpoint_dir / f"{pipeline_name}.json"
        if not path.exists():
            return None
        with open(path) as f:
            data = json.load(f)
        return PipelineCheckpoint(**data)

    def clear(self, pipeline_name: str) -> None:
        path = self.checkpoint_dir / f"{pipeline_name}.json"
        if path.exists():
            path.unlink()


def process_with_checkpoint(
    spark: SparkSession,
    pipeline_name: str,
    date_range: list[str]
):
    """チェックポイント付きの日次処理"""
    mgr = CheckpointManager("/checkpoints")
    checkpoint = mgr.load(pipeline_name)

    # チェックポイントがあれば、そこから再開
    if checkpoint and checkpoint.status == "in_progress":
        start_idx = date_range.index(checkpoint.last_processed_date) + 1
        print(f"Resuming from checkpoint: {checkpoint.last_processed_date}")
    else:
        start_idx = 0

    total_processed = checkpoint.records_processed if checkpoint else 0

    for i, date in enumerate(date_range[start_idx:], start=start_idx):
        records = process_single_day(spark, date)
        total_processed += records

        # 日次でチェックポイントを保存
        mgr.save(PipelineCheckpoint(
            pipeline_name=pipeline_name,
            last_processed_id=str(i),
            last_processed_date=date,
            records_processed=total_processed,
            status="in_progress",
            updated_at=datetime.utcnow().isoformat()
        ))

    # 完了マーク
    mgr.save(PipelineCheckpoint(
        pipeline_name=pipeline_name,
        last_processed_id=str(len(date_range) - 1),
        last_processed_date=date_range[-1],
        records_processed=total_processed,
        status="completed",
        updated_at=datetime.utcnow().isoformat()
    ))
```

### Spark Structured Streaming のチェックポイント

```python
# Structured Streaming の組み込みチェックポイント
query = (
    spark.readStream
    .format("delta")
    .table("bronze.raw_events")
    .writeStream
    .format("delta")
    .option("checkpointLocation", "/checkpoints/events_to_silver")
    .outputMode("append")
    .toTable("silver.events")
)
# チェックポイントにより、障害後も exactly-once が保証される
```

---

## 4. 冪等キーパターン

各処理リクエストに一意のキーを付与し、同じキーでの重複処理を防ぐ。

```python
from pyspark.sql.functions import col, sha2, concat_ws

def add_idempotency_key(df, key_columns: list[str]) -> "DataFrame":
    """レコードに冪等キーを付与"""
    return df.withColumn(
        "_idempotency_key",
        sha2(concat_ws("||", *[col(c).cast("string") for c in key_columns]), 256)
    )

def load_with_idempotency_key(
    spark: SparkSession,
    source_df,
    target_table: str
):
    """冪等キーによる重複排除付きロード"""
    keyed_df = add_idempotency_key(source_df, ["order_id", "product_id", "order_date"])

    # 既存の冪等キーを取得
    try:
        existing_keys = (
            spark.read.table(target_table)
            .select("_idempotency_key")
        )
        # 未処理のレコードのみ抽出
        new_records = keyed_df.join(
            existing_keys, "_idempotency_key", "left_anti"
        )
    except Exception:
        new_records = keyed_df

    new_count = new_records.count()
    if new_count > 0:
        new_records.write.mode("append").saveAsTable(target_table)
        print(f"Inserted {new_count} new records (duplicates skipped)")
    else:
        print("No new records to insert (all duplicates)")
```

---

## 5. トランザクションログパターン

処理の実行履歴をログテーブルに記録し、再実行時に処理済みかどうかを判定する。

```python
from datetime import datetime

def create_execution_log_table(spark: SparkSession):
    """実行ログテーブルの作成"""
    spark.sql("""
        CREATE TABLE IF NOT EXISTS meta.pipeline_execution_log (
            execution_id STRING,
            pipeline_name STRING,
            process_date STRING,
            batch_id STRING,
            status STRING,
            record_count BIGINT,
            started_at TIMESTAMP,
            completed_at TIMESTAMP,
            error_message STRING
        ) USING DELTA
    """)

def is_already_processed(
    spark: SparkSession,
    pipeline_name: str,
    process_date: str,
    batch_id: str
) -> bool:
    """指定バッチが処理済みかどうか"""
    result = spark.sql(f"""
        SELECT COUNT(*) AS cnt
        FROM meta.pipeline_execution_log
        WHERE pipeline_name = '{pipeline_name}'
          AND process_date = '{process_date}'
          AND batch_id = '{batch_id}'
          AND status = 'SUCCESS'
    """).collect()[0]["cnt"]

    return result > 0

def run_idempotent_pipeline(
    spark: SparkSession,
    pipeline_name: str,
    process_date: str,
    batch_id: str
):
    """トランザクションログによる冪等パイプライン"""
    if is_already_processed(spark, pipeline_name, process_date, batch_id):
        print(f"Batch {batch_id} already processed for {process_date}. Skipping.")
        return

    execution_id = f"{pipeline_name}_{process_date}_{batch_id}"
    started_at = datetime.utcnow()

    # 実行ログの開始記録
    spark.sql(f"""
        INSERT INTO meta.pipeline_execution_log VALUES (
            '{execution_id}', '{pipeline_name}', '{process_date}', '{batch_id}',
            'RUNNING', 0, timestamp('{started_at.isoformat()}'), NULL, NULL
        )
    """)

    try:
        record_count = execute_pipeline(spark, process_date)

        # 成功ログ
        spark.sql(f"""
            UPDATE meta.pipeline_execution_log
            SET status = 'SUCCESS',
                record_count = {record_count},
                completed_at = current_timestamp()
            WHERE execution_id = '{execution_id}'
        """)
    except Exception as e:
        # 失敗ログ
        spark.sql(f"""
            UPDATE meta.pipeline_execution_log
            SET status = 'FAILED',
                completed_at = current_timestamp(),
                error_message = '{str(e)[:500]}'
            WHERE execution_id = '{execution_id}'
        """)
        raise
```

---

## 6. パターン比較表

| パターン | 実装難易度 | 再実行安全性 | 適用場面 | 前提条件 |
|---------|-----------|------------|---------|---------|
| パーティション上書き | 低 | 最も安全 | 日次/月次バッチ | パーティションキーの存在 |
| MERGE/UPSERT | 中 | 安全 | マスタ同期, CDC | 主キーの存在 |
| チェックポイント | 中 | 途中再開可能 | 長時間バッチ, 大量データ | チェックポイントストレージ |
| 冪等キー | 中 | 安全 | イベント処理, API 連携 | ハッシュ計算コスト |
| トランザクションログ | 高 | 安全 | 複数ステップの複合パイプライン | ログテーブルの管理 |

### パターン選択の判断フロー

```
パイプラインの特性を確認
  ├─ 日付パーティションで分割可能？
  │     YES → パーティション上書きパターン
  │     NO ↓
  ├─ 主キーが明確？
  │     YES → MERGE/UPSERT パターン
  │     NO ↓
  ├─ レコード単位の一意キーが構成可能？
  │     YES → 冪等キーパターン
  │     NO ↓
  └─ トランザクションログパターン（汎用）

  + 長時間処理？ → チェックポイントを併用
```

---

## 7. アンチパターン

### 7.1 append のみで冪等性なし

```python
# ❌ BAD: 再実行するとデータが重複する
def load_data_bad(spark, source_df, target_table):
    source_df.write.mode("append").saveAsTable(target_table)

# ✅ GOOD: パーティション上書きで冪等性を確保
def load_data_good(spark, source_df, target_table, process_date):
    source_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("replaceWhere", f"process_date = '{process_date}'") \
        .saveAsTable(target_table)
```

### 7.2 DELETE + INSERT でトランザクション保護なし

```python
# ❌ BAD: DELETE 後に INSERT が失敗するとデータが消失する
def load_data_bad(spark, process_date):
    spark.sql(f"DELETE FROM target WHERE date = '{process_date}'")
    # ↑ ここで成功、↓ ここで失敗するとデータ消失
    new_data.write.mode("append").saveAsTable("target")

# ✅ GOOD: Delta Lake の replaceWhere はアトミック
def load_data_good(spark, process_date):
    new_data.write \
        .format("delta") \
        .mode("overwrite") \
        .option("replaceWhere", f"date = '{process_date}'") \
        .saveAsTable("target")
    # アトミックに古いデータを置換するため、中間状態でデータが消えない
```

### 7.3 外部状態に依存する冪等性

```python
# ❌ BAD: タイムスタンプに依存すると、再実行時に結果が変わる
def transform_bad(df):
    return df.withColumn("processed_at", current_timestamp())

# ✅ GOOD: 処理日パラメータを外部から注入
def transform_good(df, process_timestamp: str):
    return df.withColumn("processed_at", lit(process_timestamp))
```

---

## チェックリスト

### パイプライン設計時
- [ ] 冪等性パターンを選択し、その理由を明確にしたか
- [ ] 再実行テスト（同じ入力で2回実行）を計画したか
- [ ] 障害シナリオ（途中失敗 → 再実行）でデータが正しいことを確認したか
- [ ] アンチパターン（append only, 非トランザクション DELETE+INSERT）を避けているか

### パイプライン実装時
- [ ] パーティション上書きまたは MERGE を使用しているか
- [ ] 長時間処理にチェックポイントを設定しているか
- [ ] 処理結果のログ（実行ID, 件数, ステータス）を記録しているか
- [ ] 外部状態（現在時刻等）に依存せず、パラメータで注入しているか

### パイプライン運用時
- [ ] 再実行手順書を作成したか
- [ ] 再実行時のデータ整合性検証手順があるか
- [ ] 冪等性テストを CI/CD に組み込んだか
