# CLAUDE.md — tf-etl-env

## Project Overview

ETL/連携基盤開発向けの Claude Code 標準環境パッケージ。
tf-standard-env（共通コア）の上に、ETL 固有のエージェント・ルール・スキル・コマンドを追加する。

## Prerequisites

- tf-standard-env >= 2.0.0 がインストール済みであること

## Install

```bash
# 共通コアが未インストールの場合
cd ../10_tf-standard-env && npm run install:env

# ETL パッケージをインストール（agents / commands / skills を ~/.claude/ に配置）
npm run install:env

# プロジェクトに CLAUDE.md を初期化し、ルールを <project>/.claude/rules/ に配置
npm run install:env -- --project /path/to/etl-project
```

> **ルールはプロジェクト単位で配布します**（TFSE-009）。
> `npm run install:env` だけでは領域ルールは読み込まれません。対象プロジェクトごとに `--project` を実行してください。
> 旧方式で `~/.claude/rules/` に入った領域ルールは、`npm run install:env` 実行時に自動で削除されます。

## Components

| カテゴリ | 数 | 内容 |
|---------|---|------|
| Agents | 6 | pipeline-architect, data-quality-reviewer, schema-designer, sla-validator, etl-tdd-guide, spark-optimizer |
| Rules | 10 | pipeline-patterns, data-quality, schema-evolution, idempotency-patterns, lineage, partitioning, medallion-architecture, etl-testing, etl-security, sla-management |
| Skills | 7 | pipeline-scaffold, data-contract, migration-plan, test-data-generator, spark-tuning, incident-runbook, data-profiling |
| Commands | 5 | /pipeline-review, /dq-check, /schema-review, /etl-tdd, /lineage |

## Key Principles

1. **冪等性**: すべてのパイプラインは再実行安全に設計する
2. **Medallion Architecture**: Bronze → Silver → Gold のレイヤー構成
3. **データ品質 6次元**: 完全性, 正確性, 一貫性, 適時性, 一意性, 有効性
4. **テストファースト**: データ品質テスト → 変換テスト → 統合テスト

## Architecture

共通コア（tf-standard-env）から継承:
- SOLID, OOP, DDD 基本概念
- git-workflow, ADR, tradeoffs
- error-handling, security, observability, reliability

ETL 固有で追加:
- Medallion Architecture
- データ品質管理
- スキーマ進化戦略
- Spark 最適化
- SLA 管理
