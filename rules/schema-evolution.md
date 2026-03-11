# スキーマ進化戦略

データパイプラインにおけるスキーマ変更を安全に管理するためのルール。前方互換・後方互換の考え方から SCD の使い分け、Lakehouse でのスキーマ進化まで網羅する。

---

## 1. 互換性の分類

### 前方互換 (Forward Compatibility)

新しいスキーマで書かれたデータを、古いリーダーが読める。

```
Writer: v2 (新スキーマ) → Data → Reader: v1 (旧スキーマ)
```

**許可される変更:**
- 新フィールドの追加（古いリーダーは無視する）
- デフォルト値付きフィールドの追加

**禁止される変更:**
- 既存フィールドの削除
- 既存フィールドの型変更

### 後方互換 (Backward Compatibility)

古いスキーマで書かれたデータを、新しいリーダーが読める。

```
Writer: v1 (旧スキーマ) → Data → Reader: v2 (新スキーマ)
```

**許可される変更:**
- 新フィールドの追加（デフォルト値必須）
- オプショナルフィールドの削除

### 互換性マトリクス

| 変更タイプ | 前方互換 | 後方互換 | 完全互換 |
|-----------|---------|---------|---------|
| フィールド追加（デフォルト値あり） | OK | OK | OK |
| フィールド追加（デフォルト値なし） | OK | NG | NG |
| オプショナルフィールド削除 | NG | OK | NG |
| 必須フィールド削除 | NG | NG | NG |
| フィールドのリネーム | NG | NG | NG |
| 型の拡大（int → long） | OK | OK | OK |
| 型の縮小（long → int） | NG | NG | NG |
| Enum に値を追加 | OK | OK | OK |
| Enum から値を削除 | NG | NG | NG |

---

## 2. 破壊的変更の分類と対処法

### 破壊的変更の種類

| 変更 | リスクレベル | 対処法 |
|------|------------|--------|
| カラムの削除 | 高 | 段階的廃止（deprecation period） |
| カラム名の変更 | 高 | 新カラム追加 → 移行 → 旧カラム削除 |
| データ型の変更（縮小） | 高 | 新カラムで共存期間を設ける |
| NOT NULL 制約の追加 | 中 | デフォルト値でバックフィル後に制約追加 |
| テーブルの分割 | 高 | View で互換性を維持 |
| テーブルの統合 | 中 | 新テーブル作成 → View で旧テーブル互換 |

### 段階的廃止の手順

```python
# Phase 1: 新カラムを追加（旧カラムは維持）
def phase1_add_new_column(spark):
    spark.sql("""
        ALTER TABLE silver.customers
        ADD COLUMN full_name STRING
        COMMENT 'Replaces first_name + last_name. Migration in progress.'
    """)

    # 既存データのバックフィル
    spark.sql("""
        UPDATE silver.customers
        SET full_name = concat(first_name, ' ', last_name)
        WHERE full_name IS NULL
    """)

# Phase 2: パイプラインを新カラムに移行
# - すべての Writer が full_name を書き込む
# - すべての Reader が full_name を読み取る
# - 移行期間: 最低2週間

# Phase 3: 旧カラムを削除
def phase3_drop_old_columns(spark):
    spark.sql("""
        ALTER TABLE silver.customers
        DROP COLUMN first_name
    """)
    spark.sql("""
        ALTER TABLE silver.customers
        DROP COLUMN last_name
    """)
```

---

## 3. SCD (Slowly Changing Dimension)

ディメンションテーブルの変更履歴をどのように管理するかの戦略。

### SCD Type 比較

| 観点 | Type 1 | Type 2 | Type 3 |
|------|--------|--------|--------|
| 方式 | 上書き | 履歴行を追加 | 新旧カラム併記 |
| 変更前の値 | 失われる | 保持される | 直前の値のみ保持 |
| ストレージ消費 | 最小 | 大（行数が増加） | 中（カラム追加） |
| クエリ性能 | 最速 | 範囲フィルタが必要 | 速い |
| 実装複雑度 | 低い | 高い | 中程度 |
| ユースケース | 最新値のみ必要 | 時点復元が必要 | 前回値との比較 |
| Fact テーブルとの整合 | サロゲートキー不要 | サロゲートキー必須 | サロゲートキー不要 |

### SCD Type 1: 上書き

```sql
-- SCD Type 1: 最新値で上書き
MERGE INTO dim_customer AS target
USING staging_customer AS source
ON target.customer_id = source.customer_id
WHEN MATCHED THEN UPDATE SET
    target.customer_name = source.customer_name,
    target.email = source.email,
    target.address = source.address,
    target.updated_at = current_timestamp()
WHEN NOT MATCHED THEN INSERT (
    customer_id, customer_name, email, address, created_at, updated_at
) VALUES (
    source.customer_id, source.customer_name, source.email,
    source.address, current_timestamp(), current_timestamp()
);
```

### SCD Type 2: 履歴行追加

```python
from pyspark.sql.functions import col, lit, current_timestamp, when
from delta.tables import DeltaTable

def apply_scd_type2(
    spark,
    target_table: str,
    source_df,
    key_column: str,
    tracked_columns: list[str]
):
    """SCD Type 2 の適用: 変更があった行を期限切れにし、新しい行を追加"""
    target = DeltaTable.forName(spark, target_table)

    # 変更検出条件
    change_condition = " OR ".join([
        f"target.{c} <> source.{c}" for c in tracked_columns
    ])

    # Step 1: 変更があった既存行を期限切れにする
    target.alias("target").merge(
        source_df.alias("source"),
        f"target.{key_column} = source.{key_column} AND target.is_current = true"
    ).whenMatchedUpdate(
        condition=change_condition,
        set={
            "is_current": lit(False),
            "effective_end_date": current_timestamp(),
            "updated_at": current_timestamp()
        }
    ).execute()

    # Step 2: 新規レコードと変更レコードを INSERT
    # 既に存在しない or 変更があったレコードを特定
    existing = spark.read.table(target_table).filter(col("is_current") == True)
    new_records = source_df.join(
        existing.select(key_column, *tracked_columns),
        key_column,
        "left_anti"
    )

    changed_records = (
        source_df.alias("s").join(
            existing.alias("e"),
            f"s.{key_column} = e.{key_column}"
        ).filter(change_condition.replace("target", "e").replace("source", "s"))
        .select("s.*")
    )

    inserts = new_records.union(changed_records).withColumn(
        "is_current", lit(True)
    ).withColumn(
        "effective_start_date", current_timestamp()
    ).withColumn(
        "effective_end_date", lit(None).cast("timestamp")
    ).withColumn(
        "surrogate_key", monotonically_increasing_id()
    )

    inserts.write.mode("append").saveAsTable(target_table)
```

### SCD Type 3: 新旧カラム併記

```sql
-- SCD Type 3: 前回値と現在値を並べて保持
MERGE INTO dim_customer AS target
USING staging_customer AS source
ON target.customer_id = source.customer_id
WHEN MATCHED AND target.current_address <> source.address THEN UPDATE SET
    target.previous_address = target.current_address,
    target.current_address = source.address,
    target.address_changed_date = current_timestamp(),
    target.updated_at = current_timestamp()
WHEN NOT MATCHED THEN INSERT (
    customer_id, customer_name,
    current_address, previous_address, address_changed_date,
    created_at, updated_at
) VALUES (
    source.customer_id, source.customer_name,
    source.address, NULL, NULL,
    current_timestamp(), current_timestamp()
);
```

---

## 4. Delta Lake でのスキーマ進化

### 自動スキーマ進化

```python
# 新しいカラムを含むデータを自動的にマージ
df_with_new_column = df.withColumn("loyalty_tier", lit("SILVER"))

# mergeSchema オプションで自動進化
df_with_new_column.write \
    .format("delta") \
    .option("mergeSchema", "true") \
    .mode("append") \
    .saveAsTable("silver.customers")
```

### スキーマ上書き

```python
# スキーマを完全に置き換える（破壊的変更）
df_new_schema.write \
    .format("delta") \
    .option("overwriteSchema", "true") \
    .mode("overwrite") \
    .saveAsTable("silver.customers")
```

### Delta Lake スキーマ進化の使い分け

| オプション | 用途 | リスク |
|-----------|------|--------|
| `mergeSchema=true` | 新カラム追加 | 低（既存データに影響なし） |
| `overwriteSchema=true` | スキーマ完全置換 | 高（既存カラム定義が失われる） |
| なし（デフォルト） | スキーマ不一致時にエラー | 最も安全 |

---

## 5. Apache Iceberg でのスキーマ進化

```sql
-- Iceberg: カラム追加
ALTER TABLE catalog.db.orders ADD COLUMN priority STRING;

-- Iceberg: カラムのリネーム（メタデータのみ、データ移動なし）
ALTER TABLE catalog.db.orders RENAME COLUMN priority TO order_priority;

-- Iceberg: 型の拡大
ALTER TABLE catalog.db.orders ALTER COLUMN quantity TYPE BIGINT;

-- Iceberg: カラム順序の変更
ALTER TABLE catalog.db.orders ALTER COLUMN order_priority AFTER status;
```

---

## 6. マイグレーション手順テンプレート

### スキーマ変更のステップ

```markdown
## スキーマ変更チケット: [TICKET-ID]

### 変更内容
- テーブル: `silver.orders`
- 変更種別: カラム追加 / カラム削除 / 型変更 / リネーム
- 詳細: `shipping_method` カラム (STRING) を追加

### 互換性評価
- [ ] 前方互換: 旧リーダーが新データを読めるか → YES/NO
- [ ] 後方互換: 新リーダーが旧データを読めるか → YES/NO
- [ ] 破壊的変更か → YES/NO

### 影響範囲
- 下流テーブル: `gold.order_summary`, `gold.shipping_report`
- 下流パイプライン: `pipeline_daily_orders`, `pipeline_shipping`
- 下流ダッシュボード: `orders_dashboard`

### マイグレーション手順
1. [ ] テスト環境でスキーマ変更を適用
2. [ ] 下流パイプラインの互換性テストを実行
3. [ ] 本番環境でスキーマ変更を適用
4. [ ] 既存データのバックフィル（必要な場合）
5. [ ] 下流パイプラインのデプロイ
6. [ ] データ品質チェックの実行
7. [ ] 廃止カラムの段階的削除（スケジュール設定）

### ロールバック手順
1. [ ] タイムトラベルで変更前のバージョンに戻す
2. [ ] 下流パイプラインを前バージョンにロールバック
```

### Delta Lake タイムトラベルによるロールバック

```python
# 変更前のバージョンを確認
spark.sql("DESCRIBE HISTORY silver.orders").show()

# 特定バージョンに戻す
spark.sql("RESTORE TABLE silver.orders TO VERSION AS OF 42")

# タイムスタンプで戻す
spark.sql("RESTORE TABLE silver.orders TO TIMESTAMP AS OF '2026-03-10T12:00:00'")
```

---

## チェックリスト

### スキーマ変更時
- [ ] 変更の互換性（前方/後方）を評価したか
- [ ] 破壊的変更の場合、段階的廃止計画を策定したか
- [ ] 影響を受ける下流テーブル・パイプライン・ダッシュボードを特定したか
- [ ] マイグレーション手順書を作成したか
- [ ] ロールバック手順を確認したか
- [ ] テスト環境で事前検証を実施したか

### SCD 設計時
- [ ] ディメンションテーブルに適切な SCD Type を選択したか
- [ ] Type 2 の場合、`is_current`, `effective_start_date`, `effective_end_date` を設計したか
- [ ] サロゲートキーの採番方式を決定したか
- [ ] Fact テーブルとの結合方法を確認したか

### Lakehouse スキーマ進化
- [ ] `mergeSchema` と `overwriteSchema` の使い分けを理解しているか
- [ ] スキーマ変更のバージョン履歴が保持されているか
- [ ] タイムトラベルによるロールバックが可能か
