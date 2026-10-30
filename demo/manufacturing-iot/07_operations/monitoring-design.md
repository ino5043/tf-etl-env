# 監視設計 — 工場生産実績 ETL パイプライン

## 1. 監視アーキテクチャ

```
┌─────────┐     ┌─────────────┐     ┌──────────┐
│   MES   │ ──→ │  ETL Pipeline │ ──→ │ Lakehouse│
│  (CSV)  │     │  (Spark)      │     │ (Delta)  │
└─────────┘     └──────┬───────┘     └──────────┘
                       │
                 ┌─────┴─────┐
                 │ Monitoring │
                 └─────┬─────┘
                       │
       ┌───────────────┼───────────────┐
       ▼               ▼               ▼
┌──────────┐    ┌──────────┐    ┌──────────┐
│  Logs    │    │ Metrics  │    │  Alerts  │
│構造化JSON│    │ KPI追跡  │    │ 閾値監視 │
└──────────┘    └──────────┘    └──────────┘
```

## 2. 構造化ログ設計

### 2.1 ログフォーマット

すべてのログは JSON 構造化形式で出力する。

```json
{
  "timestamp": "2026-01-30T04:05:23.456Z",
  "level": "INFO",
  "logger": "src.transform",
  "event": "transform_completed",
  "pipeline_name": "daily_production_load",
  "batch_id": "a1b2c3d4",
  "target_date": "2026-01-29",
  "input_count": 500,
  "valid_count": 495,
  "error_count": 5,
  "error_rate": 0.01,
  "duration_ms": 12345
}
```

### 2.2 必須ログイベント

| イベント | レベル | タイミング |
|---------|--------|-----------|
| `pipeline_started` | INFO | パイプライン開始 |
| `csv_read_started` | INFO | CSV 読み込み開始 |
| `csv_read_completed` | INFO | CSV 読み込み完了 |
| `bronze_write_started` | INFO | Bronze 書き込み開始 |
| `bronze_write_completed` | INFO | Bronze 書き込み完了 |
| `transform_started` | INFO | Transform 開始 |
| `transform_completed` | INFO | Transform 完了 |
| `aggregate_started` | INFO | Aggregate 開始 |
| `aggregate_completed` | INFO | Aggregate 完了 |
| `pipeline_completed` | INFO | パイプライン完了 |
| `error_rate_exceeded` | WARN | エラー率閾値超過 |
| `pipeline_failed` | ERROR | パイプライン失敗 |

### 2.3 ログレベル使い分け

| レベル | 用途 | 例 |
|--------|------|-----|
| ERROR | 即座に対応が必要 | パイプライン失敗、Delta 書き込みエラー |
| WARN | 注意が必要 | エラー率超過、処理時間超過 |
| INFO | 正常な業務イベント | 処理開始/完了、レコード件数 |
| DEBUG | 開発・調査用 | 個別レコードの変換結果 |

## 3. メトリクス設計

### 3.1 ビジネスメトリクス

| メトリクス | 計算式 | 閾値 |
|-----------|--------|------|
| 入力レコード数 | `extract_stats.input_count` | Warning: < 400 |
| 出力レコード数 | `transform_stats.valid_count` | — |
| エラー率 | `error_count / input_count` | Warning: > 0.5%, Critical: > 1% |
| 品質スコア | `(1 - error_rate) × 100` | Warning: < 95, Critical: < 90 |
| 達成率（平均） | `AVG(achievement_rate)` | 参考値 |
| 不良率（平均） | `AVG(defect_rate)` | Warning: > 5% |

### 3.2 システムメトリクス

| メトリクス | 取得方法 | 閾値 |
|-----------|---------|------|
| パイプライン実行時間 | `end_time - start_time` | Warning: > 20min, Critical: > 30min |
| Spark ジョブ数 | Spark UI | 参考値 |
| シャッフルデータ量 | Spark UI | Warning: > 1GB |
| Executor メモリ使用率 | Spark Metrics | Warning: > 80% |
| Delta テーブルサイズ | `DESCRIBE DETAIL` | 参考値（月次レビュー） |

### 3.3 メトリクス収集例

```python
# パイプライン完了時に収集
metrics = {
    "pipeline_name": "daily_production_load",
    "timestamp": datetime.utcnow().isoformat(),
    "target_date": target_date,
    "business": {
        "input_records": result.extract_stats["input_count"],
        "output_records": result.transform_valid_count,
        "error_records": result.transform_error_count,
        "error_rate": result.transform_error_rate,
        "quality_score": round((1 - result.transform_error_rate) * 100, 2),
        "gold_kpi_rows": result.gold_kpi_count,
        "gold_health_rows": result.gold_health_count,
    },
    "system": {
        "duration_seconds": result.duration_seconds,
        "overall_status": result.overall_status,
    },
}
```

## 4. アラート設計

### 4.1 アラートルール

| # | 名前 | 条件 | 重要度 | 通知先 |
|---|------|------|--------|--------|
| 1 | パイプライン失敗 | status = "failed" | Critical | PagerDuty + Slack |
| 2 | SLA 違反リスク | AM 7:00 時点で未完了 | Warning | Slack |
| 3 | SLA 違反 | AM 8:00 時点で未完了 | Critical | PagerDuty + Email |
| 4 | エラー率超過 | error_rate > 1% | Critical | Slack + Email |
| 5 | エラー率上昇 | error_rate > 0.5% | Warning | Slack |
| 6 | レコード数異常 | input_count < 400 | Warning | Slack |
| 7 | 処理時間超過 | duration > 30min | Critical | Slack |
| 8 | 品質スコア低下 | quality_score < 90 | Critical | Slack + Email |

### 4.2 通知テンプレート

```
[CRITICAL] ETL Pipeline Alert

Pipeline: daily_production_load
Time: 2026-01-30 04:15:00 JST
Issue: Error rate exceeded threshold

Details:
- Error Rate: 2.5% (threshold: 1%)
- Input Records: 500
- Error Records: 13
- Top Error: temperature out of range (8 records)

Action Required:
1. Check Dead Letter table for error details
2. Review source data quality
3. Refer to Runbook Scenario 2

Dashboard: [Link]
Runbook: [Link]
```

## 5. ダッシュボード設計

### 5.1 運用ダッシュボード（データエンジニア向け）

```
┌─────────────────────────────────────────────────────────┐
│  Pipeline Health Dashboard                              │
├─────────────────────────────────────────────────────────┤
│                                                         │
│  [Status]        [Last Run]       [Next Run]           │
│  ● Success       04:12 AM         04:00 AM (tomorrow)  │
│                                                         │
├─────────────┬───────────────────────────────────────────┤
│  Records    │  Error Rate (30 days)                     │
│  In:  500   │  ██░░░░░░░░░░░░░░░░░░ 0.8%              │
│  Out: 495   │  ▁▁▂▁▁▁▁▃▁▁▁▁▁▂▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁       │
│  Err: 5     │                                           │
├─────────────┼───────────────────────────────────────────┤
│  Duration   │  Quality Score (30 days)                  │
│  8m 23s     │  ████████████████████ 99.0                │
│  (avg: 10m) │  ▇▇▇▇▇▇▇▆▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇       │
└─────────────┴───────────────────────────────────────────┘
```

### 5.2 ビジネスダッシュボード（工場長向け）

Power BI で以下の KPI を表示:

| KPI | 表示形式 |
|-----|---------|
| 生産達成率 | ゲージチャート + トレンド |
| 不良率 | ゲージチャート + ライン別内訳 |
| 設備健全性 | ヒートマップ（設備 × 日付） |
| データ鮮度 | 最終更新日時の表示 |

## 6. ログ保持ポリシー

| データ | 保持期間 | 理由 |
|-------|---------|------|
| 構造化ログ | 90 日 | 障害調査・トレンド分析 |
| メトリクス | 1 年 | 月次/四半期レポート |
| Dead Letter データ | 90 日 | データ品質改善分析 |
| アラート履歴 | 1 年 | SLA レポート |
| Delta テーブルバージョン | 30 日（VACUUM） | ストレージコスト最適化 |
