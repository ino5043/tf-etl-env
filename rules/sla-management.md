# SLA 管理

ETL パイプラインの SLA（Service Level Agreement）を定義・監視・報告するためのルール。データの鮮度、完全性、可用性を約束し、その達成状況を追跡する。

---

## 1. SLA 定義テンプレート

### パイプライン SLA 定義書

| 項目 | 定義 | 測定方法 |
|------|------|---------|
| パイプライン名 | `pipeline_daily_orders` | - |
| オーナー | data-engineering-team | - |
| スケジュール | 毎日 06:00 JST | cron: `0 6 * * *` |
| **データ鮮度 (Freshness)** | 毎朝 9:00 JST までに前日データが利用可能 | Gold テーブルの最新 `process_date` を確認 |
| **完全性 (Completeness)** | 欠損率 0.1% 以下 | NULL / 欠損レコード数 / 総レコード数 |
| **正確性 (Accuracy)** | データ品質スコア 95 以上 (Grade A/B) | 6次元品質スコアの加重平均 |
| **可用性 (Availability)** | 月間稼働率 99.5% | (稼働時間 / 総時間) x 100 |
| **処理時間 (Latency)** | 2時間以内に完了 | パイプライン開始〜完了の経過時間 |
| **リカバリ目標 (RTO)** | 4時間以内に復旧 | 障害検知〜サービス復旧の時間 |

### テーブル別 SLA

```python
from dataclasses import dataclass
from datetime import time

@dataclass(frozen=True)
class TableSLA:
    table_name: str
    freshness_deadline: time    # この時刻までにデータが利用可能
    max_delay_hours: int        # 最大許容遅延（時間）
    completeness_threshold: float  # 完全性の閾値（0.0 - 1.0）
    quality_score_min: float    # 品質スコアの最低値
    availability_target: float  # 可用性目標（0.0 - 1.0）

# SLA 定義
TABLE_SLAS = [
    TableSLA(
        table_name="gold.daily_sales_summary",
        freshness_deadline=time(9, 0),  # 09:00 JST
        max_delay_hours=3,
        completeness_threshold=0.999,
        quality_score_min=95.0,
        availability_target=0.995
    ),
    TableSLA(
        table_name="gold.customer_features",
        freshness_deadline=time(10, 0),  # 10:00 JST
        max_delay_hours=4,
        completeness_threshold=0.99,
        quality_score_min=90.0,
        availability_target=0.99
    ),
    TableSLA(
        table_name="silver.orders",
        freshness_deadline=time(7, 0),  # 07:00 JST
        max_delay_hours=1,
        completeness_threshold=0.999,
        quality_score_min=95.0,
        availability_target=0.999
    ),
]
```

---

## 2. モニタリング設計

### 2.1 SLA メトリクスの収集

```python
from datetime import datetime, time, timedelta
from dataclasses import dataclass, asdict
import json

@dataclass
class SLAMetrics:
    table_name: str
    check_timestamp: str
    freshness_met: bool
    freshness_actual_delay_hours: float
    completeness_rate: float
    completeness_met: bool
    quality_score: float
    quality_met: bool
    pipeline_duration_minutes: float
    latency_met: bool
    overall_sla_met: bool

def check_freshness_sla(
    spark,
    table_name: str,
    date_column: str,
    expected_date: str,
    deadline: time
) -> dict:
    """データ鮮度 SLA のチェック"""
    from pyspark.sql.functions import max as spark_max

    now = datetime.now()
    deadline_dt = datetime.combine(now.date(), deadline)

    # テーブルの最新日付を取得
    latest_date = (
        spark.read.table(table_name)
        .agg(spark_max(col(date_column)))
        .collect()[0][0]
    )

    if latest_date is None:
        return {
            "met": False,
            "latest_date": None,
            "expected_date": expected_date,
            "delay_hours": None,
            "message": "No data found in table"
        }

    is_fresh = str(latest_date) >= expected_date
    is_on_time = now <= deadline_dt if is_fresh else False
    delay_hours = max(0, (now - deadline_dt).total_seconds() / 3600) if not is_on_time else 0

    return {
        "met": is_fresh and is_on_time,
        "latest_date": str(latest_date),
        "expected_date": expected_date,
        "checked_at": now.isoformat(),
        "deadline": deadline.isoformat(),
        "delay_hours": round(delay_hours, 2),
    }


def check_completeness_sla(
    spark,
    table_name: str,
    required_columns: list[str],
    threshold: float = 0.999
) -> dict:
    """完全性 SLA のチェック"""
    df = spark.read.table(table_name)
    total = df.count()

    if total == 0:
        return {"met": False, "completeness_rate": 0, "message": "Empty table"}

    null_counts = {}
    for column in required_columns:
        null_count = df.filter(col(column).isNull()).count()
        null_counts[column] = null_count

    total_nulls = sum(null_counts.values())
    total_checks = total * len(required_columns)
    completeness_rate = (total_checks - total_nulls) / total_checks

    return {
        "met": completeness_rate >= threshold,
        "completeness_rate": round(completeness_rate, 6),
        "threshold": threshold,
        "null_counts_by_column": null_counts,
        "total_records": total
    }


def collect_sla_metrics(
    spark,
    table_sla: "TableSLA",
    pipeline_start: datetime,
    pipeline_end: datetime,
    quality_score: float,
    expected_date: str
) -> SLAMetrics:
    """SLA メトリクスの統合収集"""
    # 鮮度チェック
    freshness = check_freshness_sla(
        spark, table_sla.table_name, "process_date",
        expected_date, table_sla.freshness_deadline
    )

    # 完全性チェック
    completeness = check_completeness_sla(
        spark, table_sla.table_name,
        required_columns=["order_id", "customer_id", "order_date"],
        threshold=table_sla.completeness_threshold
    )

    # 処理時間
    duration_minutes = (pipeline_end - pipeline_start).total_seconds() / 60
    latency_met = duration_minutes <= table_sla.max_delay_hours * 60

    # 品質チェック
    quality_met = quality_score >= table_sla.quality_score_min

    # 総合判定
    overall = all([
        freshness["met"],
        completeness["met"],
        quality_met,
        latency_met
    ])

    return SLAMetrics(
        table_name=table_sla.table_name,
        check_timestamp=datetime.utcnow().isoformat(),
        freshness_met=freshness["met"],
        freshness_actual_delay_hours=freshness["delay_hours"],
        completeness_rate=completeness["completeness_rate"],
        completeness_met=completeness["met"],
        quality_score=quality_score,
        quality_met=quality_met,
        pipeline_duration_minutes=round(duration_minutes, 2),
        latency_met=latency_met,
        overall_sla_met=overall
    )
```

### 2.2 アラート設計

| メトリクス | Warning 閾値 | Critical 閾値 | 通知先 |
|-----------|-------------|--------------|--------|
| データ鮮度 | Deadline + 30分 | Deadline + 2時間 | Slack #data-alerts |
| 完全性 | < 99.9% | < 99.0% | Slack + PagerDuty |
| 品質スコア | < 95 (Grade B) | < 80 (Grade D) | Slack + PagerDuty |
| 処理時間 | > 2x 平均 | > 5x 平均 | Slack #data-alerts |
| パイプライン失敗 | - | 即時 | Slack + PagerDuty |
| 件数異常 | 前日比 +/-30% | 前日比 +/-50% | Slack #data-alerts |

```python
from enum import Enum

class AlertSeverity(Enum):
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"

@dataclass(frozen=True)
class Alert:
    severity: AlertSeverity
    title: str
    message: str
    metric_name: str
    actual_value: float
    threshold_value: float
    table_name: str
    timestamp: str

def evaluate_alerts(metrics: SLAMetrics, sla: "TableSLA") -> list[Alert]:
    """SLA メトリクスからアラートを生成"""
    alerts = []
    now = datetime.utcnow().isoformat()

    # 鮮度アラート
    if not metrics.freshness_met:
        severity = (
            AlertSeverity.CRITICAL
            if metrics.freshness_actual_delay_hours > 2
            else AlertSeverity.WARNING
        )
        alerts.append(Alert(
            severity=severity,
            title=f"Data Freshness SLA Breach: {metrics.table_name}",
            message=f"Data is {metrics.freshness_actual_delay_hours:.1f} hours behind schedule",
            metric_name="freshness_delay_hours",
            actual_value=metrics.freshness_actual_delay_hours,
            threshold_value=0,
            table_name=metrics.table_name,
            timestamp=now
        ))

    # 完全性アラート
    if not metrics.completeness_met:
        severity = (
            AlertSeverity.CRITICAL
            if metrics.completeness_rate < 0.99
            else AlertSeverity.WARNING
        )
        alerts.append(Alert(
            severity=severity,
            title=f"Completeness SLA Breach: {metrics.table_name}",
            message=f"Completeness rate {metrics.completeness_rate:.4%} below threshold {sla.completeness_threshold:.4%}",
            metric_name="completeness_rate",
            actual_value=metrics.completeness_rate,
            threshold_value=sla.completeness_threshold,
            table_name=metrics.table_name,
            timestamp=now
        ))

    # 品質スコアアラート
    if not metrics.quality_met:
        severity = (
            AlertSeverity.CRITICAL
            if metrics.quality_score < 80
            else AlertSeverity.WARNING
        )
        alerts.append(Alert(
            severity=severity,
            title=f"Quality Score SLA Breach: {metrics.table_name}",
            message=f"Quality score {metrics.quality_score:.1f} below minimum {sla.quality_score_min:.1f}",
            metric_name="quality_score",
            actual_value=metrics.quality_score,
            threshold_value=sla.quality_score_min,
            table_name=metrics.table_name,
            timestamp=now
        ))

    return alerts
```

---

## 3. エスカレーションフロー

### L1 → L2 → L3 エスカレーション

```
┌──────────────────────────────────────────────────────────────┐
│  L1: 自動対応（0〜15分）                                      │
│  ・自動リトライ（最大3回）                                     │
│  ・Slack #data-alerts に通知                                  │
│  ・ランブック自動実行（再起動、キャッシュクリア）                  │
├──────────────────────────────────────────────────────────────┤
│  L2: データエンジニア対応（15分〜2時間）                        │
│  ・エンジニアが手動調査                                        │
│  ・エラーログの分析                                            │
│  ・手動リトライ / データ修正                                    │
│  ・Slack で状況報告（30分ごと）                                 │
├──────────────────────────────────────────────────────────────┤
│  L3: 管理者・アーキテクト対応（2時間超）                        │
│  ・根本原因分析（RCA）                                        │
│  ・外部ベンダーへのエスカレーション                              │
│  ・ステークホルダーへの影響報告                                  │
│  ・恒久対策の策定                                              │
└──────────────────────────────────────────────────────────────┘
```

| レベル | 対応時間 | 対応者 | アクション |
|--------|---------|--------|-----------|
| L1 | 0〜15分 | 自動 | リトライ、通知、ランブック実行 |
| L2 | 15分〜2時間 | データエンジニア | 手動調査、修正、リトライ |
| L3 | 2時間超 | 管理者 / アーキテクト | RCA、ベンダー連絡、影響報告 |

---

## 4. 障害復旧手順テンプレート

### ランブック（Runbook）

```markdown
## 障害復旧手順: pipeline_daily_orders

### 1. 初期診断（L1: 5分以内）

1. パイプラインのステータスを確認
   ```bash
   # Airflow
   airflow dags list-runs -d pipeline_daily_orders --state failed

   # Databricks
   databricks jobs list --output json | jq '.[] | select(.settings.name == "pipeline_daily_orders")'
   ```

2. エラーログを確認
   ```bash
   # 直近のエラーログを検索
   databricks jobs get-run-output --run-id <RUN_ID>
   ```

3. エラーの分類
   - [ ] データソースの問題（接続エラー、データ遅延）
   - [ ] 変換エラー（スキーマ変更、データ品質）
   - [ ] インフラの問題（容量不足、ネットワーク）
   - [ ] 権限の問題（トークン期限切れ、アクセス拒否）

### 2. 自動リカバリ（L1: 15分以内）

1. パイプラインの再実行
   ```bash
   # 手動トリガー（冪等性が確保されているため安全）
   airflow dags trigger pipeline_daily_orders \
     --conf '{"process_date": "2026-03-10"}'
   ```

2. 再実行後のデータ検証
   ```sql
   -- レコード件数の確認
   SELECT process_date, COUNT(*) AS record_count
   FROM gold.daily_sales_summary
   WHERE process_date = '2026-03-10';

   -- 前日比の確認
   SELECT
       process_date,
       COUNT(*) AS today_count,
       LAG(COUNT(*)) OVER (ORDER BY process_date) AS yesterday_count,
       ROUND((COUNT(*) - LAG(COUNT(*)) OVER (ORDER BY process_date))
             / LAG(COUNT(*)) OVER (ORDER BY process_date) * 100, 2) AS change_pct
   FROM gold.daily_sales_summary
   GROUP BY process_date
   ORDER BY process_date DESC
   LIMIT 7;
   ```

### 3. 手動復旧（L2: 2時間以内）

1. ソースデータの確認
   ```python
   # ソースデータの到着確認
   source_files = dbutils.fs.ls("/mnt/source/orders/2026-03-10/")
   print(f"Source files: {len(source_files)}")

   # ソースデータのサンプル確認
   spark.read.csv("/mnt/source/orders/2026-03-10/").show(5)
   ```

2. Delta Lake タイムトラベルによるロールバック
   ```sql
   -- 変更前のバージョンを確認
   DESCRIBE HISTORY gold.daily_sales_summary;

   -- 正常なバージョンに戻す
   RESTORE TABLE gold.daily_sales_summary TO VERSION AS OF 42;
   ```

### 4. 復旧確認

- [ ] データが正常にロードされたか
- [ ] レコード件数が期待値と一致するか
- [ ] 品質スコアが閾値以上か
- [ ] 下流のダッシュボードが正常に表示されるか
- [ ] ステークホルダーに復旧完了を通知したか
```

---

## 5. SLA レポートテンプレート

### 週次 SLA レポート

```python
def generate_weekly_sla_report(
    spark,
    start_date: str,
    end_date: str,
    table_slas: list["TableSLA"]
) -> dict:
    """週次 SLA レポートの生成"""
    report = {
        "report_period": {"start": start_date, "end": end_date},
        "generated_at": datetime.utcnow().isoformat(),
        "summary": {},
        "tables": {}
    }

    total_checks = 0
    total_met = 0

    for sla in table_slas:
        # メトリクスの集計
        metrics_df = spark.sql(f"""
            SELECT
                COUNT(*) AS total_runs,
                SUM(CASE WHEN overall_sla_met THEN 1 ELSE 0 END) AS sla_met_count,
                AVG(freshness_actual_delay_hours) AS avg_delay_hours,
                AVG(completeness_rate) AS avg_completeness,
                AVG(quality_score) AS avg_quality_score,
                AVG(pipeline_duration_minutes) AS avg_duration_minutes,
                MAX(pipeline_duration_minutes) AS max_duration_minutes
            FROM meta.sla_metrics
            WHERE table_name = '{sla.table_name}'
              AND check_date BETWEEN '{start_date}' AND '{end_date}'
        """).collect()[0]

        total_runs = metrics_df.total_runs or 0
        sla_met = metrics_df.sla_met_count or 0
        achievement_rate = (sla_met / total_runs * 100) if total_runs > 0 else 0

        total_checks += total_runs
        total_met += sla_met

        report["tables"][sla.table_name] = {
            "total_runs": total_runs,
            "sla_met": sla_met,
            "achievement_rate": round(achievement_rate, 2),
            "avg_delay_hours": round(metrics_df.avg_delay_hours or 0, 2),
            "avg_completeness": round((metrics_df.avg_completeness or 0) * 100, 4),
            "avg_quality_score": round(metrics_df.avg_quality_score or 0, 2),
            "avg_duration_minutes": round(metrics_df.avg_duration_minutes or 0, 2),
            "max_duration_minutes": round(metrics_df.max_duration_minutes or 0, 2),
        }

    overall_rate = (total_met / total_checks * 100) if total_checks > 0 else 0
    report["summary"] = {
        "overall_achievement_rate": round(overall_rate, 2),
        "total_pipeline_runs": total_checks,
        "sla_met_count": total_met,
        "sla_breach_count": total_checks - total_met,
    }

    return report
```

### レポート出力形式

```markdown
## 週次 SLA レポート: 2026-03-04 〜 2026-03-10

### サマリー

| 指標 | 値 |
|------|-----|
| 総合 SLA 達成率 | 97.14% |
| パイプライン実行回数 | 35 |
| SLA 達成回数 | 34 |
| SLA 違反回数 | 1 |

### テーブル別詳細

| テーブル | 達成率 | 平均遅延 | 平均完全性 | 平均品質 | 平均処理時間 |
|---------|--------|---------|-----------|---------|------------|
| gold.daily_sales_summary | 100% | 0.0h | 99.98% | 97.5 | 45min |
| gold.customer_features | 85.7% | 0.3h | 99.90% | 93.2 | 62min |
| silver.orders | 100% | 0.0h | 99.99% | 98.1 | 22min |

### SLA 違反の詳細

| 日付 | テーブル | 違反種別 | 詳細 |
|------|---------|---------|------|
| 2026-03-07 | gold.customer_features | 鮮度 | ソースデータの遅延（1.5時間超過） |

### 改善アクション
1. customer_features のソースデータ到着時刻を監視に追加
2. ソースシステムチームと SLA の再調整を協議
```

---

## 6. 可用性の計算

```python
def calculate_availability(
    spark,
    table_name: str,
    start_date: str,
    end_date: str,
    expected_daily_updates: int = 1
) -> dict:
    """可用性（稼働率）の計算"""
    # 期間中の期待される更新回数
    from datetime import datetime
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    total_days = (end - start).days + 1
    expected_total = total_days * expected_daily_updates

    # 実際に成功した更新回数
    actual_success = spark.sql(f"""
        SELECT COUNT(*) AS cnt
        FROM meta.pipeline_execution_log
        WHERE table_name = '{table_name}'
          AND status = 'SUCCESS'
          AND execution_date BETWEEN '{start_date}' AND '{end_date}'
    """).collect()[0]["cnt"]

    availability = actual_success / expected_total if expected_total > 0 else 0

    return {
        "table_name": table_name,
        "period": {"start": start_date, "end": end_date},
        "expected_updates": expected_total,
        "successful_updates": actual_success,
        "failed_updates": expected_total - actual_success,
        "availability_rate": round(availability * 100, 4),
        "meets_sla": availability >= 0.995  # 99.5% 目標
    }
```

---

## チェックリスト

### SLA 定義時
- [ ] データ鮮度の期限（Freshness Deadline）を定義したか
- [ ] 完全性の閾値（Completeness Threshold）を定義したか
- [ ] 品質スコアの最低値（Quality Score Minimum）を定義したか
- [ ] 可用性目標（Availability Target）を定義したか
- [ ] 処理時間の上限（Latency Limit）を定義したか

### モニタリング設計時
- [ ] 各 SLA メトリクスの収集ジョブを設定したか
- [ ] アラートの閾値（Warning / Critical）を設定したか
- [ ] エスカレーションフロー（L1 → L2 → L3）を定義したか
- [ ] 通知先（Slack, PagerDuty 等）を設定したか

### 運用時
- [ ] 障害復旧手順書（ランブック）を作成したか
- [ ] ランブックを定期的にテスト（訓練）しているか
- [ ] 週次/月次の SLA レポートを生成しているか
- [ ] SLA 違反の根本原因分析（RCA）を実施しているか
- [ ] SLA の定期見直し（四半期ごと）を計画しているか
