---
name: data-contract
description: Define and validate data contracts using YAML-based schema definitions with quality rules for producer-consumer agreements.
---

# Data Contract

プロデューサーとコンシューマー間のデータ品質・スキーマに関する合意を、YAML ベースのデータコントラクトとして定義・検証する。

---

## データコントラクトとは

データコントラクトは、データのプロデューサー（生産者）とコンシューマー（消費者）の間で交わされる正式な合意文書である。以下を定義する。

| 項目 | 説明 |
|------|------|
| スキーマ定義 | カラム名、データ型、制約 |
| 品質ルール | NULL 率上限、一意性、値域、鮮度 |
| SLA | データ提供のタイミングと可用性 |
| バージョニング | スキーマ変更時の互換性ルール |
| オーナーシップ | 責任者、連絡先、エスカレーションパス |

---

## YAML データコントラクトテンプレート

```yaml
# data-contract.yaml
apiVersion: v1.0.0
kind: DataContract

metadata:
  name: orders-daily
  version: "1.2.0"
  description: "日次注文データ"
  owner:
    team: data-engineering
    contact: data-eng@example.com
  tags:
    - orders
    - daily
    - silver

producer:
  system: order-service
  database: order_db
  table: orders
  format: delta
  update_frequency: daily

consumer:
  - name: analytics-dashboard
    team: analytics
    usage: KPI集計
  - name: ml-pipeline
    team: data-science
    usage: 需要予測モデル

schema:
  type: record
  fields:
    - name: order_id
      type: string
      description: "注文ID（UUID形式）"
      required: true
      unique: true
      pattern: "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"

    - name: customer_id
      type: string
      description: "顧客ID"
      required: true

    - name: order_date
      type: date
      description: "注文日"
      required: true
      format: "YYYY-MM-DD"

    - name: total_amount
      type: decimal
      description: "注文合計金額"
      required: true
      precision: 10
      scale: 2
      constraints:
        minimum: 0

    - name: status
      type: string
      description: "注文ステータス"
      required: true
      enum:
        - PENDING
        - CONFIRMED
        - SHIPPED
        - DELIVERED
        - CANCELLED

    - name: updated_at
      type: timestamp
      description: "最終更新日時"
      required: true

quality_rules:
  completeness:
    - field: order_id
      null_rate_max: 0.0
    - field: customer_id
      null_rate_max: 0.0
    - field: total_amount
      null_rate_max: 0.01

  uniqueness:
    - fields: [order_id]
      scope: global

  freshness:
    max_delay_hours: 24
    reference_field: updated_at

  volume:
    min_records_per_day: 100
    max_records_per_day: 1000000
    anomaly_threshold_percent: 50

  accuracy:
    - field: total_amount
      rule: "total_amount >= 0"
    - field: order_date
      rule: "order_date <= current_date()"

sla:
  availability: 99.5
  delivery_time: "06:00 JST"
  support_hours: "09:00-18:00 JST"

versioning:
  strategy: semantic
  backward_compatible: true
  deprecation_notice_days: 30
```

---

## バージョニング戦略

| 変更種別 | バージョン | 互換性 | 例 |
|---------|-----------|--------|-----|
| パッチ | x.x.+1 | 完全互換 | 説明文の修正 |
| マイナー | x.+1.0 | 後方互換 | オプショナルカラムの追加 |
| メジャー | +1.0.0 | 非互換 | 必須カラムの削除、型変更 |

### 互換性ルール

- **後方互換**: 新バージョンのデータを旧コンシューマーが読める
- **前方互換**: 旧バージョンのデータを新コンシューマーが読める
- 必須カラムの削除・型変更は **メジャーバージョンアップ** が必要
- メジャーバージョンアップ時は **deprecation_notice_days** 前に通知する

---

## Python 検証スクリプト

```python
"""
Data Contract Validator
データコントラクトに基づいてデータの品質を検証する。
"""
import yaml
from dataclasses import dataclass, field
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from typing import Any
import logging

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ValidationResult:
    """検証結果"""
    rule_name: str
    field_name: str
    passed: bool
    actual_value: Any
    expected_value: Any
    message: str


@dataclass
class ContractValidationReport:
    """検証レポート"""
    contract_name: str
    contract_version: str
    results: list[ValidationResult] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return all(r.passed for r in self.results)

    @property
    def summary(self) -> dict:
        total = len(self.results)
        passed = sum(1 for r in self.results if r.passed)
        return {
            "contract": self.contract_name,
            "version": self.contract_version,
            "total_rules": total,
            "passed": passed,
            "failed": total - passed,
            "pass_rate": round(passed / total * 100, 2) if total > 0 else 0,
        }


def load_contract(path: str) -> dict:
    """YAML データコントラクトを読み込む"""
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def validate_completeness(
    df: DataFrame, rules: list[dict]
) -> list[ValidationResult]:
    """完全性（NULL 率）を検証する"""
    results = []
    total_count = df.count()

    for rule in rules:
        field_name = rule["field"]
        max_null_rate = rule["null_rate_max"]
        null_count = df.filter(F.col(field_name).isNull()).count()
        actual_null_rate = null_count / total_count if total_count > 0 else 0

        results.append(ValidationResult(
            rule_name="completeness",
            field_name=field_name,
            passed=actual_null_rate <= max_null_rate,
            actual_value=round(actual_null_rate, 4),
            expected_value=max_null_rate,
            message=f"NULL rate: {actual_null_rate:.4f} (max: {max_null_rate})",
        ))

    return results


def validate_uniqueness(
    df: DataFrame, rules: list[dict]
) -> list[ValidationResult]:
    """一意性を検証する"""
    results = []

    for rule in rules:
        fields = rule["fields"]
        total_count = df.count()
        distinct_count = df.select(fields).distinct().count()

        results.append(ValidationResult(
            rule_name="uniqueness",
            field_name=", ".join(fields),
            passed=total_count == distinct_count,
            actual_value=distinct_count,
            expected_value=total_count,
            message=f"Distinct: {distinct_count} / Total: {total_count}",
        ))

    return results


def validate_contract(
    df: DataFrame, contract: dict
) -> ContractValidationReport:
    """データコントラクト全体を検証する"""
    metadata = contract["metadata"]
    quality_rules = contract.get("quality_rules", {})

    report = ContractValidationReport(
        contract_name=metadata["name"],
        contract_version=metadata["version"],
    )

    if "completeness" in quality_rules:
        report.results.extend(
            validate_completeness(df, quality_rules["completeness"])
        )

    if "uniqueness" in quality_rules:
        report.results.extend(
            validate_uniqueness(df, quality_rules["uniqueness"])
        )

    logger.info("Contract validation completed", extra=report.summary)
    return report
```

---

## 検証の実行手順

1. データコントラクト YAML ファイルを作成する
2. プロデューサーがデータを生成する
3. 検証スクリプトを実行してコントラクトに適合するか確認する
4. 検証レポートを確認し、違反があればプロデューサーに通知する
5. コンシューマーはコントラクトに基づいてデータを利用する

---

## 注意事項

- データコントラクトはバージョン管理リポジトリで管理する
- コントラクトの変更はプロデューサーとコンシューマーの合意のもとで行う
- 品質ルールの閾値は、実データのプロファイリング結果に基づいて設定する
- CI/CD パイプラインにコントラクト検証を組み込み、違反時にアラートを出す
