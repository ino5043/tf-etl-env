---
name: incident-runbook
description: Generate incident runbooks for ETL pipeline failures with root cause analysis templates and recovery procedures.
---

# Incident Runbook

ETL パイプラインの障害対応ランブックを生成する。根本原因分析（RCA）テンプレート、復旧手順、再発防止策を含む。

---

## 障害対応フロー

```
検知 → トリアージ → 調査 → 復旧 → RCA → 再発防止
```

| フェーズ | 目標時間 | 担当 | 成果物 |
|---------|---------|------|--------|
| 検知 | 即時（自動アラート） | 監視システム | アラート通知 |
| トリアージ | 15分以内 | オンコール担当 | 重要度判定 |
| 調査 | 1時間以内 | データエンジニア | 原因特定 |
| 復旧 | SLA に準拠 | データエンジニア | サービス回復 |
| RCA | 3営業日以内 | チーム | RCA レポート |
| 再発防止 | 2週間以内 | チーム | 改善実装 |

---

## 重要度分類

| 重要度 | 基準 | 対応 | SLA |
|--------|------|------|-----|
| P1 (Critical) | ビジネスKPIに直接影響、データ損失の可能性 | 即時対応、エスカレーション | 4時間以内に復旧 |
| P2 (High) | SLA違反、複数パイプライン停止 | 優先対応 | 8時間以内に復旧 |
| P3 (Medium) | 単一パイプライン遅延、品質劣化 | 通常対応 | 24時間以内に復旧 |
| P4 (Low) | 影響なし、軽微な警告 | 次営業日対応 | 1週間以内に対応 |

---

## よくある障害パターンと対応

### 1. データソース障害

**症状**: 抽出フェーズでタイムアウトまたは接続エラー

```
エラー例:
- ConnectionTimeoutError: Could not connect to source_db:5432
- HTTPError: 503 Service Unavailable
- FileNotFoundError: Source file not found at /data/input/
```

**調査手順**:
1. データソースの稼働状態を確認する
2. ネットワーク接続を確認する（ping, telnet）
3. 認証情報の有効期限を確認する
4. ソース側のログを確認する

**復旧手順**:
1. ソースが復旧するまで待機する
2. パイプラインを再実行する（冪等性により安全）
3. 遅延が SLA を超える場合はエスカレーションする

### 2. スキーマ変更（Breaking Change）

**症状**: 変換フェーズで AnalysisException または型不一致エラー

```
エラー例:
- AnalysisException: Cannot resolve column 'old_column_name'
- TypeError: Cannot cast string to integer for column 'amount'
- SchemaValidationError: Missing required field 'customer_id'
```

**調査手順**:
1. ソーステーブルの現在のスキーマを確認する
2. データコントラクトと比較する
3. 変更されたカラムを特定する
4. 変更がいつ行われたか確認する

**復旧手順**:
1. スキーマ変更の内容を確認する
2. パイプラインのスキーママッピングを修正する
3. テスト環境で修正を検証する
4. 本番に反映して再実行する

### 3. リソース不足（Out of Memory）

**症状**: Executor が OOM でクラッシュ

```
エラー例:
- OutOfMemoryError: Java heap space
- Container killed by YARN for exceeding memory limits
- SparkException: Task failed due to executor lost
```

**調査手順**:
1. Spark UI で Executor のメモリ使用量を確認する
2. GC ログを確認する
3. データ量が想定を超えていないか確認する
4. データスキューが発生していないか確認する

**復旧手順**:
1. Executor メモリを増加する（executor.memory, memoryOverhead）
2. パーティション数を増やす（shuffle.partitions）
3. ブロードキャスト結合の閾値を下げる
4. 必要に応じてデータを分割して処理する

### 4. タイムアウト

**症状**: パイプラインが SLA 時間内に完了しない

```
エラー例:
- TimeoutError: Pipeline exceeded maximum execution time
- AirflowTaskTimeout: Task timed out after 7200 seconds
```

**調査手順**:
1. 各ステージの処理時間を確認する
2. ボトルネックのステージを特定する
3. データ量の増加が原因か確認する
4. リソース競合がないか確認する

**復旧手順**:
1. タイムアウト値を一時的に延長する
2. ボトルネックを `/spark-tuning` スキルで最適化する
3. 必要に応じてパイプラインを分割する

---

## 根本原因分析（RCA）テンプレート

```markdown
# RCA Report: INC-YYYY-NNN

## インシデント概要
| 項目 | 値 |
|------|-----|
| インシデント ID | INC-YYYY-NNN |
| 発生日時 | YYYY-MM-DD HH:MM (JST) |
| 検知日時 | YYYY-MM-DD HH:MM (JST) |
| 復旧日時 | YYYY-MM-DD HH:MM (JST) |
| 影響時間 | N 時間 M 分 |
| 重要度 | P1 / P2 / P3 / P4 |
| 影響範囲 | [影響を受けたシステム・データ] |
| 対応者 | [対応者名] |

## タイムライン
| 時刻 | イベント | アクション |
|------|---------|-----------|
| HH:MM | アラート発報 | オンコール担当に通知 |
| HH:MM | 調査開始 | ログ確認、原因特定 |
| HH:MM | 復旧開始 | [復旧アクション] |
| HH:MM | 復旧完了 | サービス正常化確認 |

## 根本原因
[障害の根本原因を記述]

## 5 Whys 分析
1. なぜ障害が発生したか？ → [回答]
2. なぜ [1の回答] が起きたか？ → [回答]
3. なぜ [2の回答] が起きたか？ → [回答]
4. なぜ [3の回答] が起きたか？ → [回答]
5. なぜ [4の回答] が起きたか？ → [回答（根本原因）]

## 復旧手順（実施した内容）
1. [手順1]
2. [手順2]
3. [手順3]

## 再発防止策
| # | 対策 | 担当 | 期限 | ステータス |
|---|------|------|------|-----------|
| 1 | [対策内容] | [担当者] | YYYY-MM-DD | 未着手/進行中/完了 |
| 2 | [対策内容] | [担当者] | YYYY-MM-DD | 未着手/進行中/完了 |

## 教訓（Lessons Learned）
- [教訓1]
- [教訓2]
```

---

## エスカレーションフロー

```
Level 1: オンコールエンジニア
  ├─ 15分以内に応答
  ├─ P3/P4: 自力で対応
  └─ P1/P2: Level 2 にエスカレーション
         │
Level 2: シニアエンジニア / テックリード
  ├─ 30分以内に対応開始
  ├─ 1時間以内に原因特定
  └─ 復旧見込みなし: Level 3 にエスカレーション
         │
Level 3: マネージャー + ベンダーサポート
  ├─ ステークホルダーへの状況報告
  └─ 外部ベンダーとの連携
```

---

## 復旧手順テンプレート

```markdown
# Recovery Procedure: [パイプライン名]

## 前提条件
- [ ] データソースが正常に稼働していること
- [ ] 必要な権限があること
- [ ] バックアップが取得済みであること

## 復旧手順

### Step 1: 状態確認
```bash
# パイプラインの最終実行状態を確認
SELECT * FROM pipeline_run_log
WHERE pipeline_name = '[パイプライン名]'
ORDER BY started_at DESC LIMIT 5;
```

### Step 2: データ整合性確認
```bash
# ターゲットテーブルの最新データを確認
SELECT MAX(process_date), COUNT(*) FROM target_table;
```

### Step 3: 再実行
```bash
# 冪等なので安全に再実行可能
python -m etl.pipeline --date YYYY-MM-DD --force
```

### Step 4: 検証
```bash
# 処理結果の確認
SELECT process_date, COUNT(*), SUM(amount)
FROM target_table
WHERE process_date = 'YYYY-MM-DD'
GROUP BY process_date;
```
```

---

## ランブック生成の実行手順

1. 対象パイプラインの構成情報を収集する
2. データソース、ターゲット、依存関係を特定する
3. 想定される障害パターンを列挙する
4. 各障害パターンに対する調査・復旧手順を作成する
5. エスカレーションフローを定義する
6. RCA テンプレートを生成する

---

## 注意事項

- ランブックは定期的に（四半期ごと）レビュー・更新する
- 障害訓練（Game Day）を実施して手順の有効性を検証する
- 復旧手順は必ずテスト環境で事前に検証する
- ランブックは全チームメンバーがアクセス可能な場所に格納する
