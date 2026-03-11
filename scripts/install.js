#!/usr/bin/env node
/**
 * tf-etl-env インストールスクリプト
 *
 * Usage:
 *   node scripts/install.js                        # ~/.claude/ に agents/rules/commands/skills を同期
 *   node scripts/install.js --dry-run              # プレビュー（変更なし）
 *   node scripts/install.js --uninstall            # アンインストール
 *   node scripts/install.js --project <path>       # プロジェクトの CLAUDE.md に ETL セクションを追記
 *
 * 前提条件:
 *   tf-standard-env（共通コア）が先にインストールされていること。
 *   共通ルール（SOLID, git-workflow, ADR 等）は tf-standard-env から提供される。
 */

const fs = require('fs');
const path = require('path');
const os = require('os');

const CLAUDE_DIR = path.join(os.homedir(), '.claude');
const SOURCE_DIR = path.resolve(__dirname, '..');

const COMPONENTS = [
  { src: 'agents', dest: 'agents', label: 'Agents (ETL)' },
  { src: 'rules', dest: 'rules', label: 'Rules (ETL)' },
  { src: 'commands', dest: 'commands', label: 'Commands (ETL)' },
  { src: 'skills', dest: 'skills', label: 'Skills (ETL)' },
];

function parseArgs() {
  const args = process.argv.slice(2);
  const projectIdx = args.indexOf('--project');
  return {
    dryRun: args.includes('--dry-run'),
    uninstall: args.includes('--uninstall'),
    project: projectIdx !== -1 ? args[projectIdx + 1] : null,
  };
}

function ensureDir(dirPath) {
  if (!fs.existsSync(dirPath)) {
    fs.mkdirSync(dirPath, { recursive: true });
  }
}

function copyDir(src, dest, dryRun) {
  const files = fs.readdirSync(src);
  let copied = 0;

  for (const file of files) {
    const srcPath = path.join(src, file);
    const destPath = path.join(dest, file);
    const stat = fs.statSync(srcPath);

    if (stat.isDirectory()) {
      if (!dryRun) {
        ensureDir(destPath);
      }
      copied += copyDir(srcPath, destPath, dryRun);
    } else {
      const action = fs.existsSync(destPath) ? 'update' : 'create';
      console.log(`  ${action === 'create' ? '+' : '~'} ${path.relative(CLAUDE_DIR, destPath)}`);
      if (!dryRun) {
        ensureDir(path.dirname(destPath));
        fs.copyFileSync(srcPath, destPath);
      }
      copied++;
    }
  }
  return copied;
}

function removeDir(dirPath, basePath) {
  if (!fs.existsSync(dirPath)) {
    return 0;
  }
  const files = fs.readdirSync(dirPath);
  let removed = 0;

  for (const file of files) {
    const filePath = path.join(dirPath, file);
    const stat = fs.statSync(filePath);

    if (stat.isDirectory()) {
      removed += removeDir(filePath, basePath);
      if (fs.readdirSync(filePath).length === 0) {
        fs.rmdirSync(filePath);
      }
    } else {
      console.log(`  - ${path.relative(basePath, filePath)}`);
      fs.unlinkSync(filePath);
      removed++;
    }
  }
  return removed;
}

function checkTfseInstalled() {
  const tfseMarkers = [
    path.join(CLAUDE_DIR, 'rules', 'solid-principles.md'),
    path.join(CLAUDE_DIR, 'rules', 'git-workflow.md'),
    path.join(CLAUDE_DIR, 'agents', 'planner.md'),
  ];

  const installed = tfseMarkers.some(f => fs.existsSync(f));
  if (!installed) {
    console.warn('\n⚠ tf-standard-env（共通コア）が検出されませんでした。');
    console.warn('  先に tf-standard-env をインストールしてください:');
    console.warn('  cd tf-standard-env && npm run install:env\n');
  }
  return installed;
}

function install(options) {
  const { dryRun } = options;
  const mode = dryRun ? '[DRY RUN] ' : '';

  console.log(`\n${mode}tf-etl-env インストール`);
  console.log(`${'─'.repeat(40)}`);
  console.log(`ソース: ${SOURCE_DIR}`);
  console.log(`宛先:   ${CLAUDE_DIR}`);
  console.log();

  checkTfseInstalled();

  let totalFiles = 0;

  for (const comp of COMPONENTS) {
    const srcPath = path.join(SOURCE_DIR, comp.src);
    const destPath = path.join(CLAUDE_DIR, comp.dest);

    if (!fs.existsSync(srcPath)) {
      console.log(`⚠ ${comp.label}: ソースが見つかりません (${srcPath})`);
      continue;
    }

    console.log(`${comp.label}:`);
    if (!dryRun) {
      ensureDir(destPath);
    }
    const count = copyDir(srcPath, destPath, dryRun);
    totalFiles += count;
    console.log();
  }

  console.log(`${'─'.repeat(40)}`);
  console.log(`${mode}${totalFiles} ファイルを処理しました`);

  if (dryRun) {
    console.log('\n--dry-run なし で実行すると実際にインストールされます');
  }
}

function uninstall() {
  console.log('\ntf-etl-env アンインストール');
  console.log(`${'─'.repeat(40)}`);

  // ETL 固有ファイルのみ削除（共通コアは残す）
  const etlFiles = [];
  for (const comp of COMPONENTS) {
    const srcPath = path.join(SOURCE_DIR, comp.src);
    if (!fs.existsSync(srcPath)) continue;

    const files = getAllFiles(srcPath);
    for (const file of files) {
      const relative = path.relative(srcPath, file);
      const destFile = path.join(CLAUDE_DIR, comp.dest, relative);
      if (fs.existsSync(destFile)) {
        etlFiles.push(destFile);
      }
    }
  }

  let removed = 0;
  for (const file of etlFiles) {
    console.log(`  - ${path.relative(CLAUDE_DIR, file)}`);
    fs.unlinkSync(file);
    removed++;
  }

  console.log(`${'─'.repeat(40)}`);
  console.log(`${removed} ファイルを削除しました（共通コアは維持）`);
}

function getAllFiles(dir) {
  const files = [];
  for (const entry of fs.readdirSync(dir)) {
    const full = path.join(dir, entry);
    if (fs.statSync(full).isDirectory()) {
      files.push(...getAllFiles(full));
    } else {
      files.push(full);
    }
  }
  return files;
}

const TFSE_ETL_MARKER = '## Development Workflow（TFSE-ETL 準拠）';

const TFSE_ETL_SECTION = `
---

## Development Workflow（TFSE-ETL 準拠）

このプロジェクトには TFSE-ETL がインストールされています。
共通ルールは tf-standard-env、ETL 固有ルールは tf-etl-env から提供されます。

### ETL ワークフロー

| ステップ | コマンド | 用途 |
|---------|---------|------|
| 1. 計画 | \`/plan\` | パイプライン設計を確認してから着手 |
| 2. パイプラインレビュー | \`/pipeline-review\` | アーキテクチャ + データ品質レビュー |
| 3. スキーマレビュー | \`/schema-review\` | スキーマ設計の妥当性確認 |
| 4. TDD | \`/etl-tdd\` | テストファースト（品質テスト含む） |
| 5. 品質チェック | \`/dq-check\` | データ品質ルールの生成・レビュー |
| 6. リネージ | \`/lineage\` | データフローの可視化 |

### パイプライン設計原則

- **冪等性**: すべてのパイプラインは再実行安全
- **Medallion**: Bronze → Silver → Gold のレイヤー構成
- **データ品質**: 6次元（完全性, 正確性, 一貫性, 適時性, 一意性, 有効性）

### Git Workflow

- **ブランチ戦略**: \`feature/xxx\` → PR → \`main\`
- **コミット形式**: \`feat:\`, \`fix:\`, \`refactor:\`, \`docs:\`, \`test:\`, \`chore:\`
`;

function initProject(projectPath) {
  const targetPath = path.resolve(projectPath);

  if (!fs.existsSync(targetPath)) {
    console.error(`\nエラー: プロジェクトパスが見つかりません: ${targetPath}`);
    process.exit(1);
  }

  const claudeMdPath = path.join(targetPath, 'CLAUDE.md');

  console.log(`\ntf-etl-env プロジェクト初期化`);
  console.log(`${'─'.repeat(40)}`);
  console.log(`プロジェクト: ${targetPath}`);
  console.log();

  if (fs.existsSync(claudeMdPath)) {
    const content = fs.readFileSync(claudeMdPath, 'utf8');
    if (content.includes(TFSE_ETL_MARKER)) {
      console.log(`= CLAUDE.md: TFSE-ETL セクション既存 → スキップ（冪等）`);
    } else {
      fs.appendFileSync(claudeMdPath, TFSE_ETL_SECTION, 'utf8');
      console.log(`+ CLAUDE.md: TFSE-ETL セクションを追記`);
    }
  } else {
    fs.writeFileSync(claudeMdPath, `# CLAUDE.md\n${TFSE_ETL_SECTION}`, 'utf8');
    console.log(`+ CLAUDE.md: TFSE-ETL テンプレートで新規作成`);
  }

  console.log();
  console.log(`${'─'.repeat(40)}`);
  console.log(`完了！次のセッションから /pipeline-review, /etl-tdd 等が使えます。`);
}

// Main
const options = parseArgs();

if (options.uninstall) {
  uninstall();
} else if (options.project) {
  initProject(options.project);
} else {
  install(options);
}
