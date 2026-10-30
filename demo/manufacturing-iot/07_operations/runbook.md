# 障害対応ランブック — 工場生産実績 ETL パイプライン

| 項目 | 値 |
|------|-----|
| パイプライン名 | daily_production_load |
| スケジュール | 毎日 AM 4:00 JST |
| SLA | AM 8:00 JST |
| 最大許容遅延 | AM 10:00 JST |
| エスカレーション先 | データエンジニアリングチーム当番 |

---

## シナリオ 1: ソース CSV 未着

### 症状

- パイプラインが「ファイル未検出」エラーで失敗
- ログ: `FileNotFoundException: production_{date}.csv not found`

### 影響度

**HIGH** — ダッシュボードに前日データが表示されない

### 診断手順

```bash
# 1. ソースファイルの存在確認
az storage blob list \
  --account-name factorylakehouse \
  --container-name raw \
  --prefix mes/production/production_$(date -d yesterday +%Y%m%d)

# 2. SFTP 転送ログの確認
az monitor activity-log list \
  --resource-group factory-rg \
  --start-time $(date -d "6 hours ago" --iso-8601) \
  | jq '.[] | select(.operationName.value | contains("blob"))'

# 3. MES システムの稼働状況確認
# → MES チーム連絡先: mes-support@example.com
```

### 復旧手順

1. **MES チームに連絡** — ファイル転送状況を確認
2. **手動ファイルアップロード**（MES チームからファイルを受領した場合）

    ```bash
    az storage blob upload \
      --account-name factorylakehouse \
      --container-name raw \
      --name mes/production/production_{date}.csv \
      --file /path/to/received/file.csv
    ```

3. **パイプライン再実行**

    ```bash
    # Fabric Notebook から再実行
    # パラメータ: target_date = "YYYY-MM-DD"
    ```

4. **暫定対応**（ファイル入手に時間がかかる場合）
   - 工場長に遅延を通知
   - 前日のダッシュボードデータは変更なしで維持

### 予防策

- SFTP 転送の監視アラートを追加
- AM 3:30 にファイル存在チェックジョブを追加

---

## シナリオ 2: 品質スコア閾値超過

### 症状

- パイプラインは完了するが、品質スコアが閾値を超過
- ログ: `Error rate exceeded threshold: error_rate=0.05, threshold=0.01`
- Dead Letter テーブルに大量の不合格レコード

### 影響度

**MEDIUM** — データは取り込まれるが、一部欠損の可能性

### 診断手順

```sql
-- 1. Dead Letter テーブルでエラー内容を確認
SELECT
    production_date,
    line_id,
    status,
    temperature,
    vibration,
    _rejected_at,
    _rejection_reason
FROM meta.dead_letter_production
WHERE _rejected_at >= CURRENT_DATE - INTERVAL 1 DAY
ORDER BY _rejected_at DESC
LIMIT 100;

-- 2. エラーパターンの分析
SELECT
    CASE
        WHEN production_date IS NULL THEN 'NULL_DATE'
        WHEN line_id IS NULL OR TRIM(line_id) = '' THEN 'EMPTY_LINE'
        WHEN status NOT IN ('正常','警告','異常','停止') THEN 'INVALID_STATUS'
        WHEN temperature NOT BETWEEN 20.0 AND 80.0 THEN 'TEMP_OUT_OF_RANGE'
        WHEN defect_quantity > actual_quantity THEN 'DEFECT_EXCEEDS_ACTUAL'
        ELSE 'OTHER'
    END AS error_category,
    COUNT(*) AS error_count
FROM meta.dead_letter_production
WHERE _rejected_at >= CURRENT_DATE - INTERVAL 1 DAY
GROUP BY 1
ORDER BY 2 DESC;
```

### 復旧手順

1. **エラーパターンを特定** — 上記 SQL を実行
2. **パターン別対応**

    | パターン | 対応 |
    |---------|------|
    | `NULL_DATE` | MES の日付出力フォーマット変更を確認 |
    | `INVALID_STATUS` | MES のステータスマスタ変更を確認 |
    | `TEMP_OUT_OF_RANGE` | IoT センサーの校正確認 |
    | `DEFECT_EXCEEDS_ACTUAL` | MES の入力バリデーション確認 |

3. **データ修正後に再実行**（冪等性により安全）

    ```bash
    # 修正済み CSV で再実行
    # target_date パーティションが上書きされる
    ```

4. **品質ルール調整が必要な場合**
   - `04_quality/quality-rules.yaml` の閾値を調整
   - ADR を作成して変更理由を記録

### 予防策

- 品質スコアの週次トレンド分析
- MES のマスタデータ変更時の事前通知フロー確立

---

## シナリオ 3: Spark ジョブ OOM（メモリ不足）

### 症状

- パイプラインが `OutOfMemoryError` で失敗
- ログ: `java.lang.OutOfMemoryError: Java heap space`
- または `ExecutorLostFailure (executor X exited caused by one of the running tasks)`

### 影響度

**HIGH** — パイプライン全体が停止

### 診断手順

```bash
# 1. Spark UI でメモリ使用状況を確認
# Fabric ポータル → モニタリング → Apache Spark アプリケーション

# 2. 入力データ量の確認
az storage blob list \
  --account-name factorylakehouse \
  --container-name raw \
  --prefix mes/production/ \
  --output table \
  | sort -k 3 -rn \
  | head -5

# 3. Delta テーブルのサイズ確認
# Spark SQL で実行
# DESCRIBE DETAIL bronze.raw_production
```

### 復旧手順

1. **即時対応 — Spark 設定調整**

    ```python
    # Notebook セルで設定変更
    spark.conf.set("spark.sql.shuffle.partitions", "8")  # 2 → 8
    spark.conf.set("spark.executor.memory", "4g")  # デフォルトから増加
    spark.conf.set("spark.driver.memory", "4g")
    ```

2. **データ量が急増した場合**
   - 日付を分割して処理

    ```python
    # 大量データを分割処理
    for chunk_date in date_range(start, end):
        run_pipeline(spark, chunk_date, config)
    ```

3. **根本対応 — パーティション戦略見直し**
   - `pipeline_config.yaml` の `shuffle.partitions` を恒久的に調整
   - Fabric 容量のスケールアップを検討（F2 → F4）

4. **再実行**

    ```bash
    # 冪等性により安全に再実行可能
    # target_date を指定して実行
    ```

### 予防策

- 月次のデータ量トレンド分析（月 +3% 成長を想定）
- Spark メモリ使用量のアラート設定
- 半年ごとの容量キャパシティプランニング

---

## エスカレーションマトリクス

| 時間経過 | アクション | 通知先 |
|---------|-----------|--------|
| 0 分 | 自動リトライ（最大 3 回） | — |
| 15 分 | Slack #data-alert に通知 | データエンジニア |
| 30 分 | PagerDuty アラート | 当番エンジニア |
| 60 分（AM 7:00） | 工場長に遅延可能性を通知 | 工場長、生産管理課 |
| 120 分（AM 8:00） | SLA 違反、インシデント起票 | マネージャー |
| 360 分（AM 10:00） | 最大許容遅延超過、緊急対応 | 部長 |

---

## 定期メンテナンス

| 項目 | 頻度 | 担当 |
|------|------|------|
| Dead Letter テーブルのクリーンアップ | 月次 | データエンジニア |
| Delta テーブルの OPTIMIZE / VACUUM | 週次 | 自動ジョブ |
| SLA 達成率レビュー | 月次 | チームリーダー |
| 容量キャパシティプランニング | 四半期 | データエンジニア + インフラ |
| 品質ルール見直し | 四半期 | データエンジニア + 品質管理 |
