#!/usr/bin/env node
// 製造業 IoT デモを Microsoft Fabric に自動配置する（deploy-guide.md 手順 2〜9 の自動化）
//
// Usage:
//   cp config.example.sh config.sh   # 値を編集する（config.sh は実行されず、KEY="value" の行だけが読まれる）
//   node --use-system-ca deploy.js all                 # すべて実行（./deploy.sh all も同じ）
//   node --use-system-ca deploy.js <step> [<step> ...]  # 手順を選んで実行
//   --use-system-ca: OS の証明書ストアを信頼する（HTTPS 検査のある環境向け。README の「はまりどころ」参照）
//
// 手順: workspace lakehouse upload notebook run verify model report pipeline shots
// 作成した項目の ID は $WORK_DIR/state.json に保存され、後続の手順や再実行で使われる。ログは JSON Lines で標準エラーに出る。
"use strict";
const fs = require("node:fs");
const path = require("node:path");
const { spawn } = require("node:child_process");
const { loadConfig } = require("./lib/config");
const { createLogger } = require("./lib/log");
const { createHttpClient } = require("./lib/http");
const { createTokenProvider } = require("./lib/auth");
const { createApiClient } = require("./lib/api");
const { createFabricClient } = require("./lib/fabric");
const { createOneLakeClient } = require("./lib/onelake");
const { createPowerBiClient } = require("./lib/powerbi");
const { createStateStore, checkWorkspaceState, STATE_FILE } = require("./lib/state");
const { AUDIENCES } = require("./lib/endpoints");
const { STEPS } = require("./lib/steps");
const { STEP_ORDER, resolveSteps, runSteps, errorFields } = require("./lib/runner");

const HERE = __dirname;
const USAGE = `usage: node deploy.js all | <step> [<step> ...]
steps: ${STEP_ORDER.join(" ")}
env:   CONFIG（設定ファイル。既定 automation/config.sh）, WORK_DIR（作業フォルダー。既定 automation/.work）, BROWSER_PATH / SHOTS_TIMEOUT_MS（shots。既定 300000）
`;

const SHOTS_TIMEOUT_MS = 5 * 60_000;
const SHOTS_TIMEOUT_EXIT = 124; // timeout(1) と同じ慣例の終了コード

// 画面キャプチャは puppeteer を使う別プロセスで撮る。トークンは環境変数で渡す（コマンドラインには載せない）。
// ブラウザーが固まっても止まらないよう、全体のタイムアウトで子プロセスを終了させる
const defaultRunShots = (tokens, env, { spawnImpl = spawn, timeoutMs = SHOTS_TIMEOUT_MS, logger = null } = {}) => async ({ workspaceId, reportId, outDir, pages }) => {
  const token = await tokens.getToken(AUDIENCES.powerbi);
  const script = path.join(HERE, "scripts", "shoot_report.js");
  return new Promise((resolve) => {
    let timedOut = false;
    const child = spawnImpl(process.execPath, [script, workspaceId, reportId, outDir, ...pages], { env: { ...env, PBI_TOKEN: token }, stdio: ["ignore", "inherit", "inherit"], windowsHide: true });
    const timer = setTimeout(() => {
      timedOut = true;
      if (logger) logger.error("shots_timeout", { timeout_ms: timeoutMs });
      child.kill("SIGKILL");
    }, timeoutMs);
    const done = (code) => { clearTimeout(timer); resolve(code); };
    child.on("error", () => done(1));
    child.on("exit", (code) => done(timedOut ? SHOTS_TIMEOUT_EXIT : code === null ? 1 : code));
  });
};

const shotsTimeoutFrom = (env) => (/^\d+$/.test(env.SHOTS_TIMEOUT_MS || "") ? Number(env.SHOTS_TIMEOUT_MS) : SHOTS_TIMEOUT_MS);

function createContext({ config, env, write, deps = {} }) {
  const logger = createLogger({ write });
  const http = createHttpClient({ logger, ...(deps.fetchImpl ? { fetchImpl: deps.fetchImpl } : {}) });
  const tokens = deps.tokens || createTokenProvider({ env, caBundle: config.caBundle });
  const api = createApiClient({ http, tokens });
  return Object.freeze({
    config,
    paths: Object.freeze({ here: HERE, fabricDir: path.join(HERE, ".."), workDir: config.workDir }),
    store: createStateStore(config.workDir),
    logger,
    fabric: createFabricClient({ api, logger }),
    onelake: createOneLakeClient({ api }),
    powerbi: createPowerBiClient({ api, logger }),
    runShots: deps.runShots || defaultRunShots(tokens, env, { logger, timeoutMs: shotsTimeoutFrom(env) }),
  });
}

async function main(argv, { env = process.env, write = (s) => process.stderr.write(s), cwd = process.cwd(), deps = {} } = {}) {
  const logger = createLogger({ write });
  if (argv.length === 0 || ["-h", "--help", "help"].includes(argv[0])) {
    write(USAGE);
    return argv.length === 0 ? 1 : 0;
  }
  let names;
  try { names = resolveSteps(argv); } catch (e) {
    logger.error("deploy_failed", { status: "failed", ...errorFields(e) });
    return 2;
  }
  try {
    const config = loadConfig({ configPath: env.CONFIG || path.join(HERE, "config.sh"), env, defaultWorkDir: path.join(HERE, ".work"), cwd });
    fs.mkdirSync(config.workDir, { recursive: true });
    const ctx = (deps.createContext || createContext)({ config, env, write, deps });
    if (config.unknownKeys.length) ctx.logger.warn("config_unknown_keys", { keys: config.unknownKeys });
    // 別のワークスペース用の state.json で実行すると、他人の項目を更新しかねないので止める
    if (checkWorkspaceState(ctx.store.get(), config.workspaceName) === "unverified") {
      ctx.logger.warn("state_workspace_unverified", { workspace: config.workspaceName, state_file: path.join(config.workDir, STATE_FILE) });
    }
    ctx.logger.info("deploy_started", { steps: names, workspace: config.workspaceName, work_dir: config.workDir });
    await runSteps(names, deps.steps || STEPS, ctx);
    ctx.logger.info("deploy_completed", { status: "ok", state_file: path.join(config.workDir, STATE_FILE), ids: ctx.store.get() });
    return 0;
  } catch (e) {
    logger.error("deploy_failed", { status: "failed", step: e.step, ...errorFields(e) });
    return 1;
  }
}

if (require.main === module) {
  process.on("unhandledRejection", (e) => {
    createLogger().error("deploy_failed", { status: "failed", ...errorFields(e instanceof Error ? e : new Error(String(e))) });
    process.exit(1);
  });
  main(process.argv.slice(2)).then((code) => { process.exitCode = code; });
}

module.exports = { main, createContext, defaultRunShots, USAGE };
