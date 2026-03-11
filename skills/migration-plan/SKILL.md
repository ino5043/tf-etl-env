---
name: migration-plan
description: Create schema migration plans with impact analysis, rollback procedures, and step-by-step execution guides.
---

# Migration Plan

スキーママイグレーションの計画策定、影響分析、ロールバック手順、段階的移行パターンを提供する。

---

## マイグレーション計画策定プロセス

以下のステップに従ってマイグレーション計画を策定する。

| ステップ | 内容 | 成果物 |
|---------|------|--------|
| 1. 要件整理 | 変更内容と目的の明確化 | 変更要件書 |
| 2. 影響分析 | 依存関係の洗い出しと影響範囲の特定 | 影響分析レポート |
| 3. 移行戦略選定 | 移行パターンの選択 | 戦略文書 |
| 4. ロールバック計画 | 失敗時の復旧手順 | ロールバック手順書 |
| 5. テスト計画 | 移行前後のデータ検証 | テスト計画書 |
| 6. 実行計画 | タイムライン、担当者、チェックポイント | 実行計画書 |
| 7. 事後確認 | 移行完了の検証 | 完了報告書 |

---

## 影響分析（Impact Analysis）

スキーマ変更が影響するコンポーネントを網羅的に特定する。

### 分析対象

| 対象 | 確認内容 |
|------|---------|
| 下流テーブル | 変更カラムを参照するビュー、マテリアライズドビュー |
| ETL パイプライン | 変更テーブルを読み書きするジョブ |
| データコントラクト | 影響するコントラクトの互換性 |
| BI レポート | 変更カラムを使用するダッシュボード |
| API | 変更テーブルを参照するエンドポイント |
| ML モデル | 変更カラムを特徴量として使用するモデル |

### 影響分析クエリ例

```sql
-- 変更テーブルを参照するビューの特定
SELECT
    v.table_schema,
    v.table_name,
    v.view_definition
FROM information_schema.views v
WHERE v.view_definition LIKE '%target_table_name%';

-- 外部キー制約の確認
SELECT
    tc.constraint_name,
    tc.table_name,
    kcu.column_name,
    ccu.table_name AS foreign_table_name,
    ccu.column_name AS foreign_column_name
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu
    ON tc.constraint_name = kcu.constraint_name
JOIN information_schema.constraint_column_usage ccu
    ON ccu.constraint_name = tc.constraint_name
WHERE tc.constraint_type = 'FOREIGN KEY'
    AND (tc.table_name = 'target_table_name'
         OR ccu.table_name = 'target_table_name');
```

---

## 段階的移行パターン

### 1. Dual-Write パターン

新旧両方のスキーマに書き込み、段階的に切り替える。

```
Phase 1: 旧テーブルのみに書き込み
Phase 2: 新旧両方に書き込み（Dual-Write）
Phase 3: 読み取りを新テーブルに切り替え
Phase 4: 旧テーブルへの書き込みを停止
Phase 5: 旧テーブルの廃止（猶予期間後）
```

**適用条件**: ダウンタイム不可、データ整合性が最優先

### 2. Shadow パターン

新スキーマに並行してデータを複製し、検証後に切り替える。

```
Phase 1: 新テーブルを作成し、既存データをコピー
Phase 2: 新テーブルにシャドウ書き込みを開始
Phase 3: 新旧データの整合性を検証
Phase 4: 読み取りを新テーブルに切り替え
Phase 5: シャドウ書き込みを停止
```

**適用条件**: データ量が大きく、一括移行が困難

### 3. Strangler Fig パターン

新スキーマを段階的に導入し、旧スキーマを徐々に置き換える。

```
Phase 1: 新機能は新スキーマで実装
Phase 2: 既存機能を段階的に新スキーマに移行
Phase 3: 旧スキーマのアクセスをプロキシ経由に変更
Phase 4: 旧スキーマを廃止
```

**適用条件**: 長期的なリファクタリング、モノリス分割

### パターン比較

| 観点 | Dual-Write | Shadow | Strangler Fig |
|------|-----------|--------|---------------|
| ダウンタイム | なし | なし | なし |
| 実装複雑度 | 高 | 中 | 中 |
| データ整合性 | 高（要検証） | 高 | 中 |
| 移行期間 | 短〜中 | 中 | 長 |
| ストレージコスト | 2倍（一時的） | 2倍（一時的） | 段階的増加 |

---

## ロールバック手順

### ロールバック判断基準

| 条件 | アクション |
|------|-----------|
| データ不整合率 > 1% | ロールバック実行 |
| 下流パイプライン失敗 | 原因調査 → 修正不可ならロールバック |
| パフォーマンス劣化 > 50% | ロールバック検討 |
| SLA 違反 | 即座にロールバック |

### ロールバック手順テンプレート

```sql
-- 1. マイグレーション状態の確認
SELECT * FROM migration_log
WHERE migration_id = 'MIG-2026-001'
ORDER BY executed_at DESC;

-- 2. ロールバック DDL の実行
-- ※ 事前に準備しておいたロールバックスクリプト
BEGIN TRANSACTION;

-- カラム追加のロールバック例
ALTER TABLE orders DROP COLUMN IF EXISTS new_column;

-- カラム名変更のロールバック例
ALTER TABLE orders RENAME COLUMN new_name TO old_name;

-- 型変更のロールバック例
ALTER TABLE orders ALTER COLUMN amount TYPE INTEGER;

COMMIT;

-- 3. データ整合性の確認
SELECT COUNT(*) AS total_records,
       COUNT(DISTINCT order_id) AS unique_orders
FROM orders;

-- 4. 下流パイプラインの再実行
-- ※ 影響分析で特定したパイプラインを再実行
```

---

## Migration Plan テンプレート

```markdown
# Migration Plan: MIG-YYYY-NNN

## 概要
| 項目 | 値 |
|------|-----|
| マイグレーション ID | MIG-YYYY-NNN |
| 対象テーブル | schema.table_name |
| 変更内容 | [カラム追加/型変更/テーブル分割] |
| 移行パターン | [Dual-Write/Shadow/Strangler Fig] |
| 予定日 | YYYY-MM-DD |
| 担当者 | [名前] |
| 承認者 | [名前] |

## 変更詳細
[変更の詳細を記述]

## 影響分析結果
| 影響対象 | 影響度 | 対応 |
|---------|--------|------|
| [対象1] | 高/中/低 | [対応内容] |

## 実行手順
1. [ ] 事前バックアップの取得
2. [ ] DDL の実行
3. [ ] データマイグレーションの実行
4. [ ] 整合性チェックの実行
5. [ ] 下流パイプラインの動作確認
6. [ ] BI レポートの動作確認

## ロールバック手順
1. [ ] ロールバック DDL の実行
2. [ ] データ整合性の確認
3. [ ] 下流パイプラインの再実行

## チェックポイント
| タイミング | 確認内容 | 合格基準 |
|-----------|---------|---------|
| DDL 実行後 | テーブル構造 | 期待するスキーマと一致 |
| データ移行後 | レコード数 | 移行前と一致 |
| 切り替え後 | パイプライン | 全ジョブが成功 |
```

---

## ダウンタイム最小化戦略

| 戦略 | 説明 | 適用場面 |
|------|------|---------|
| Online DDL | ロックなしでスキーマ変更 | カラム追加、インデックス追加 |
| Blue-Green | 新旧環境を切り替え | テーブルリネーム、大規模変更 |
| Feature Flag | フラグで新旧ロジックを切り替え | アプリケーション層の変更 |
| Backfill 分離 | DDL とデータ移行を分離 | 大量データの型変換 |

---

## 注意事項

- マイグレーション実行前に必ずバックアップを取得する
- ロールバックスクリプトは事前にテスト環境で検証する
- 影響分析は自動化ツール（data lineage）と手動確認の両方で行う
- 本番実行時はメンテナンスウィンドウ内で実施する
- 実行ログを migration_log テーブルに記録し、監査証跡を残す
