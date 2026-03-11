# ETL セキュリティ

ETL パイプラインにおけるデータ保護、アクセス制御、監査ログの設計に関するルール。データの機密性・完全性・可用性を確保する。

---

## 1. データマスキングパターン

### Static Masking（静的マスキング）

データをコピーする際に不可逆的にマスキングする。テスト環境や開発環境で使用。

```python
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sha2, concat, lit, regexp_replace, when, length, substring

def mask_pii_static(df: DataFrame) -> DataFrame:
    """PII データの静的マスキング（不可逆）"""
    return (
        df
        # メールアドレス: ハッシュ化
        .withColumn(
            "email",
            concat(
                sha2(col("email"), 256).substr(1, 12),
                lit("@masked.example.com")
            )
        )
        # 氏名: 姓の一部をマスク
        .withColumn(
            "customer_name",
            concat(
                substring(col("customer_name"), 1, 1),
                lit("***")
            )
        )
        # 電話番号: 下4桁以外をマスク
        .withColumn(
            "phone",
            concat(
                lit("***-****-"),
                substring(col("phone"), length(col("phone")) - 3, 4)
            )
        )
        # 住所: 市区町村以降をマスク
        .withColumn(
            "address",
            regexp_replace(col("address"), r"(.*?[市区町村郡]).*", "$1***")
        )
        # クレジットカード: 下4桁のみ表示
        .withColumn(
            "credit_card",
            concat(
                lit("****-****-****-"),
                substring(col("credit_card"), -4, 4)
            )
        )
    )
```

### Dynamic Masking（動的マスキング）

データは原本のまま保持し、クエリ時にユーザーの権限に応じてマスキングする。

```sql
-- Databricks Unity Catalog: Dynamic View による動的マスキング
CREATE OR REPLACE VIEW silver.customers_masked AS
SELECT
    customer_id,
    -- 管理者はフルネーム、一般ユーザーはマスク
    CASE
        WHEN is_member('data_admin_group') THEN customer_name
        ELSE concat(substring(customer_name, 1, 1), '***')
    END AS customer_name,
    -- 管理者はメール表示、一般ユーザーはマスク
    CASE
        WHEN is_member('data_admin_group') THEN email
        ELSE concat('***@', substring_index(email, '@', -1))
    END AS email,
    region,
    customer_segment,
    created_at
FROM silver.customers;
```

```sql
-- Delta Lake Row/Column Security (Unity Catalog)
-- カラムマスクの設定
ALTER TABLE silver.customers
ALTER COLUMN email
SET MASK mask_email_function;

-- マスク関数の定義
CREATE OR REPLACE FUNCTION mask_email_function(email STRING)
RETURNS STRING
RETURN
    CASE
        WHEN is_member('pii_access_group') THEN email
        ELSE concat('***@', substring_index(email, '@', -1))
    END;
```

### マスキング方式の比較

| 観点 | Static Masking | Dynamic Masking |
|------|---------------|-----------------|
| データの変更 | 不可逆的に変更 | 原本は変更なし |
| 用途 | テスト/開発環境 | 本番環境 |
| 性能影響 | なし（事前処理） | クエリ時にオーバーヘッド |
| 柔軟性 | 低い（再生成が必要） | 高い（権限で制御） |
| リスク | 元データは別途保護 | 設定ミスで漏洩リスク |

---

## 2. PII（個人情報）の分類と保護レベル

| 分類 | 保護レベル | データ例 | 必要な保護措置 |
|------|-----------|---------|--------------|
| 高機密 | 最高 | マイナンバー, クレジットカード番号, パスワード | 暗号化 + マスキング + アクセス制限 + 監査ログ |
| 機密 | 高 | 氏名, メールアドレス, 電話番号, 住所, 生年月日 | マスキング + アクセス制限 + 監査ログ |
| 内部 | 中 | 社員ID, 部署名, 職位 | アクセス制限 |
| 公開 | 低 | 商品名, カテゴリ, 集計値 | 特になし |

### PII 自動検出

```python
import re

# PII パターンの定義
PII_PATTERNS = {
    "email": r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}",
    "phone_jp": r"0\d{1,4}-?\d{1,4}-?\d{4}",
    "credit_card": r"\d{4}[-\s]?\d{4}[-\s]?\d{4}[-\s]?\d{4}",
    "my_number": r"\d{12}",  # マイナンバー
    "postal_code_jp": r"\d{3}-?\d{4}",
}

# カラム名パターンによる PII 検出
PII_COLUMN_PATTERNS = {
    "high": [
        r".*credit.*card.*", r".*ssn.*", r".*my_?number.*",
        r".*password.*", r".*secret.*",
    ],
    "medium": [
        r".*name.*", r".*email.*", r".*phone.*", r".*address.*",
        r".*birth.*date.*", r".*dob.*",
    ],
    "low": [
        r".*employee.*id.*", r".*dept.*", r".*department.*",
    ],
}

def detect_pii_columns(df) -> dict[str, list[str]]:
    """DataFrame のカラム名から PII の可能性があるカラムを検出"""
    results = {"high": [], "medium": [], "low": []}

    for column in df.columns:
        col_lower = column.lower()
        for level, patterns in PII_COLUMN_PATTERNS.items():
            for pattern in patterns:
                if re.match(pattern, col_lower):
                    results[level].append(column)
                    break

    return results

# 使用例
pii_report = detect_pii_columns(df)
# {'high': ['credit_card_number'], 'medium': ['customer_name', 'email'], 'low': ['employee_id']}
```

---

## 3. 暗号化

### 3.1 At Rest（保管時の暗号化）

```python
# Delta Lake: テーブルレベルの暗号化（クラウドプロバイダーの機能を利用）

# Azure: Storage Account の暗号化（デフォルトで有効）
# - Microsoft Managed Keys（デフォルト）
# - Customer Managed Keys（Azure Key Vault）

# AWS: S3 の暗号化
# - SSE-S3（デフォルト）
# - SSE-KMS（AWS KMS）
# - CSE（クライアントサイド暗号化）

# Spark 設定例（AWS SSE-KMS）
spark.conf.set("spark.hadoop.fs.s3a.server-side-encryption-algorithm", "SSE-KMS")
spark.conf.set("spark.hadoop.fs.s3a.server-side-encryption.key", "arn:aws:kms:region:account:key/key-id")
```

### 3.2 In Transit（転送中の暗号化）

```python
# JDBC 接続: TLS/SSL の強制
jdbc_url = (
    "jdbc:postgresql://db-host:5432/mydb"
    "?ssl=true"
    "&sslmode=verify-full"
    "&sslrootcert=/path/to/ca-cert.pem"
)

df = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "public.orders") \
    .option("user", db_user) \
    .option("password", db_password) \
    .load()
```

### 3.3 In Use（処理中の暗号化）

```python
from pyspark.sql.functions import sha2, col, concat

# カラムレベルの暗号化（ハッシュ化）
# ※ 不可逆のため、結合キーとして使用する場合
def hash_sensitive_column(df, column: str, salt: str) -> "DataFrame":
    """機密カラムのハッシュ化（不可逆）"""
    return df.withColumn(
        column,
        sha2(concat(col(column), lit(salt)), 256)
    )

# AES 暗号化（可逆）
# Spark 3.3+ の組み込み関数
from pyspark.sql.functions import aes_encrypt, aes_decrypt, unbase64, base64

def encrypt_column(df, column: str, key: str) -> "DataFrame":
    """AES-256 によるカラム暗号化"""
    return df.withColumn(
        column,
        base64(aes_encrypt(col(column), lit(key), lit("GCM")))
    )

def decrypt_column(df, column: str, key: str) -> "DataFrame":
    """AES-256 によるカラム復号"""
    return df.withColumn(
        column,
        aes_decrypt(unbase64(col(column)), lit(key), lit("GCM")).cast("string")
    )
```

---

## 4. アクセス制御

### RBAC（ロールベースアクセス制御）

```sql
-- Unity Catalog: ロールベースのアクセス制御

-- グループの作成（管理コンソールで実施）
-- data_engineers: ETL パイプラインの実行権限
-- data_analysts: Silver/Gold の読み取り権限
-- data_admins: 全権限 + PII アクセス

-- テーブルレベルの権限
GRANT SELECT ON TABLE silver.orders TO data_analysts;
GRANT SELECT, MODIFY ON TABLE silver.orders TO data_engineers;
GRANT ALL PRIVILEGES ON TABLE silver.orders TO data_admins;

-- スキーマレベルの権限
GRANT USAGE ON SCHEMA gold TO data_analysts;
GRANT SELECT ON ALL TABLES IN SCHEMA gold TO data_analysts;

-- Bronze レイヤーはエンジニアのみ
GRANT USAGE ON SCHEMA bronze TO data_engineers;
DENY SELECT ON SCHEMA bronze TO data_analysts;
```

### Column-level Security（カラムレベルセキュリティ）

```sql
-- 特定カラムへのアクセスを制限
-- Unity Catalog のカラムマスクを使用
ALTER TABLE silver.customers
ALTER COLUMN email SET MASK mask_email;

ALTER TABLE silver.customers
ALTER COLUMN phone SET MASK mask_phone;

-- 権限を持つグループのみ元データにアクセス可能
```

### Row-level Security（行レベルセキュリティ）

```sql
-- 行フィルタの設定（Unity Catalog）
ALTER TABLE silver.orders
SET ROW FILTER region_filter ON (region);

-- フィルタ関数の定義
CREATE OR REPLACE FUNCTION region_filter(region STRING)
RETURNS BOOLEAN
RETURN
    CASE
        WHEN is_member('global_data_access') THEN TRUE
        WHEN is_member('apac_team') AND region = 'APAC' THEN TRUE
        WHEN is_member('emea_team') AND region = 'EMEA' THEN TRUE
        ELSE FALSE
    END;
```

---

## 5. 接続文字列・シークレットの管理

### Key Vault / Secrets Manager の使用

```python
# ❌ BAD: ハードコードされた認証情報
db_password = "my-secret-password"
api_key = "sk-proj-xxxxx"

# ✅ GOOD: Azure Key Vault からシークレットを取得
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential

def get_secret(vault_url: str, secret_name: str) -> str:
    """Azure Key Vault からシークレットを取得"""
    credential = DefaultAzureCredential()
    client = SecretClient(vault_url=vault_url, credential=credential)
    secret = client.get_secret(secret_name)
    return secret.value

# 使用例
db_password = get_secret(
    "https://my-keyvault.vault.azure.net",
    "db-password"
)
```

```python
# ✅ GOOD: AWS Secrets Manager からシークレットを取得
import boto3
import json

def get_secret_aws(secret_name: str, region: str = "ap-northeast-1") -> dict:
    """AWS Secrets Manager からシークレットを取得"""
    client = boto3.client("secretsmanager", region_name=region)
    response = client.get_secret_value(SecretId=secret_name)
    return json.loads(response["SecretString"])

# 使用例
db_creds = get_secret_aws("prod/database/orders")
db_password = db_creds["password"]
```

```python
# ✅ GOOD: Databricks Secrets
db_password = dbutils.secrets.get(scope="production", key="db-password")

# Spark JDBC 接続
df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://host:5432/db") \
    .option("user", dbutils.secrets.get(scope="production", key="db-user")) \
    .option("password", dbutils.secrets.get(scope="production", key="db-password")) \
    .option("dbtable", "public.orders") \
    .load()
```

### シークレット管理のルール

| ルール | 説明 |
|--------|------|
| ハードコード禁止 | コード内に認証情報を直接書かない |
| 環境変数の利用 | ローカル開発時は `.env` + `python-dotenv` |
| Key Vault の利用 | 本番環境では Key Vault / Secrets Manager を使用 |
| ローテーション | シークレットは定期的にローテーションする |
| 最小権限 | シークレットへのアクセスは必要最小限の権限で |
| 監査ログ | シークレットへのアクセスをログに記録する |

---

## 6. 監査ログの設計

```python
from dataclasses import dataclass, asdict
from datetime import datetime
import json

@dataclass(frozen=True)
class AuditLogEntry:
    event_type: str       # "DATA_ACCESS", "DATA_MODIFY", "SCHEMA_CHANGE", "EXPORT"
    timestamp: str
    user_id: str
    action: str           # "READ", "WRITE", "DELETE", "ALTER"
    resource: str         # テーブル名
    details: dict
    source_ip: str | None = None
    result: str = "SUCCESS"  # "SUCCESS", "DENIED", "ERROR"

def log_data_access(
    user_id: str,
    table_name: str,
    action: str,
    query: str | None = None,
    row_count: int | None = None,
    pii_accessed: bool = False
) -> AuditLogEntry:
    """データアクセスの監査ログを記録"""
    entry = AuditLogEntry(
        event_type="DATA_ACCESS",
        timestamp=datetime.utcnow().isoformat(),
        user_id=user_id,
        action=action,
        resource=table_name,
        details={
            "query": query[:500] if query else None,
            "row_count": row_count,
            "pii_accessed": pii_accessed,
        }
    )

    # 構造化ログとして出力
    print(json.dumps(asdict(entry), ensure_ascii=False))
    return entry
```

### 監査ログの必須項目

| 項目 | 説明 | 例 |
|------|------|-----|
| event_type | イベントの分類 | DATA_ACCESS, DATA_MODIFY |
| timestamp | イベント発生日時（UTC） | 2026-03-11T10:30:00Z |
| user_id | 操作者の識別子 | user@example.com |
| action | 具体的な操作 | READ, WRITE, DELETE |
| resource | 対象リソース | silver.customers |
| result | 操作の結果 | SUCCESS, DENIED |
| pii_accessed | PII にアクセスしたか | true/false |
| details | 追加情報 | クエリ, 件数 |

---

## 7. GDPR / 個人情報保護法への対応パターン

### 忘れられる権利（Right to be Forgotten）

```python
def delete_customer_data(
    spark,
    customer_id: str,
    tables: list[str]
):
    """顧客データの削除（GDPR 対応）"""
    deletion_log = []

    for table_name in tables:
        try:
            # Delta Lake の DELETE
            spark.sql(f"""
                DELETE FROM {table_name}
                WHERE customer_id = '{customer_id}'
            """)

            # VACUUM で物理削除（タイムトラベルからも削除）
            spark.sql(f"VACUUM {table_name} RETAIN 0 HOURS")

            deletion_log.append({
                "table": table_name,
                "customer_id": customer_id,
                "status": "DELETED",
                "timestamp": datetime.utcnow().isoformat()
            })
        except Exception as e:
            deletion_log.append({
                "table": table_name,
                "customer_id": customer_id,
                "status": "FAILED",
                "error": str(e),
                "timestamp": datetime.utcnow().isoformat()
            })

    return deletion_log
```

### データポータビリティ（Right to Data Portability）

```python
def export_customer_data(
    spark,
    customer_id: str,
    output_path: str
) -> dict:
    """顧客データのエクスポート（GDPR 対応）"""
    tables = ["silver.customers", "silver.orders", "silver.interactions"]
    export_results = {}

    for table_name in tables:
        entity = table_name.split(".")[-1]
        df = spark.read.table(table_name).filter(
            col("customer_id") == customer_id
        )
        count = df.count()

        if count > 0:
            export_file = f"{output_path}/{entity}.json"
            df.write.mode("overwrite").json(export_file)
            export_results[entity] = {
                "record_count": count,
                "export_path": export_file
            }

    return export_results
```

---

## チェックリスト

### データ保護設計時
- [ ] PII カラムを特定し、機密度レベルを分類したか
- [ ] マスキング方式（Static / Dynamic）を決定したか
- [ ] 暗号化方式（At Rest / In Transit / In Use）を設定したか
- [ ] 非本番環境でのデータマスキングルールを定義したか

### アクセス制御
- [ ] RBAC のロールとグループを定義したか
- [ ] テーブル/カラム/行レベルのアクセス制御を設定したか
- [ ] Bronze レイヤーへのアクセスをエンジニアに限定したか
- [ ] PII カラムへのアクセスを特権グループに限定したか

### シークレット管理
- [ ] コード内にハードコードされた認証情報がないか
- [ ] Key Vault / Secrets Manager を使用しているか
- [ ] シークレットのローテーションポリシーがあるか

### 監査・コンプライアンス
- [ ] データアクセスの監査ログを記録しているか
- [ ] PII アクセスの追跡が可能か
- [ ] GDPR の削除要求に対応する手順があるか
- [ ] データポータビリティの手順があるか
