// Power BI レポートを JS ライブラリで埋め込み表示し、各ページを PNG で撮影する（ヘッドレスの Chromium 系ブラウザー）
// Usage: node shoot_report.js <groupId> <reportId> <outDir> <pageNames...>   （環境変数 PBI_TOKEN に AAD トークン）
// powerbi-client はローカルの node_modules から読み込む（CDN は使わない）。1 ページでも描画できなければ終了コード 1
"use strict";
const path = require("path");
const fs = require("fs");

// 外部スクリプトを一切読み込まない空のページ。powerbi-client は addScriptTag でローカルファイルから注入する
const PAGE_HTML = `<!doctype html><html><head><meta charset="utf-8">
<style>html,body{margin:0;background:#fff}#r{width:1280px;height:720px}</style></head>
<body><div id="r"></div></body></html>`;

const BROWSER_DEFAULTS = Object.freeze({
  win32: ["C:/Program Files (x86)/Microsoft/Edge/Application/msedge.exe", "C:/Program Files/Microsoft/Edge/Application/msedge.exe", "C:/Program Files/Google/Chrome/Application/chrome.exe"],
  darwin: ["/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge", "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"],
  linux: ["/usr/bin/microsoft-edge", "/usr/bin/google-chrome", "/usr/bin/chromium", "/usr/bin/chromium-browser"],
});

function resolveBrowserPath({ env = process.env, platform = process.platform, exists = fs.existsSync } = {}) {
  if (env.BROWSER_PATH) return env.BROWSER_PATH;
  const found = (BROWSER_DEFAULTS[platform] || []).find((p) => exists(p));
  if (!found) throw new Error("Chromium 系ブラウザーが見つからない。BROWSER_PATH にブラウザーの実行ファイルを設定する");
  return found;
}

// --no-sandbox は Linux を root で動かす場合（コンテナー等）だけ付ける
// ブラウザーには PBI_TOKEN を除いた環境変数を明示的に渡す（トークンはページ内の埋め込み呼び出しにだけ渡す）
const withoutToken = (env) => Object.fromEntries(Object.entries(env).filter(([k]) => k !== "PBI_TOKEN"));

function launchOptions({ browserPath, platform = process.platform, isRoot = false, env = process.env }) {
  return { executablePath: browserPath, headless: true, args: platform === "linux" && isRoot ? ["--no-sandbox"] : [], env: withoutToken(env), defaultViewport: { width: 1280, height: 720, deviceScaleFactor: 2 } };
}

function resolvePowerBiClient(resolve = require.resolve) {
  for (const id of ["powerbi-client/dist/powerbi.min.js", "powerbi-client"]) {
    try { return resolve(id); } catch { /* 次の候補を試す */ }
  }
  throw new Error("powerbi-client が見つからない（automation フォルダーで npm install を実行する）");
}

// puppeteer-core の場所: 環境変数 PUPPETEER_CORE_PATH > 通常の require > mermaid-cli に同梱のもの（Windows のグローバルインストール）
function loadPuppeteer(env = process.env) {
  const candidates = [env.PUPPETEER_CORE_PATH, "puppeteer-core", env.APPDATA && path.join(env.APPDATA, "npm/node_modules/@mermaid-js/mermaid-cli/node_modules/puppeteer-core")].filter(Boolean);
  for (const c of candidates) { try { return require(c); } catch { /* 次の候補を試す */ } }
  throw new Error("puppeteer-core が見つからない（automation フォルダーで npm install、または PUPPETEER_CORE_PATH を設定）");
}

// ブラウザー内で実行される関数（Node 側の変数は参照できない）
/* node:coverage disable */
async function embedAndWait(token, group, report, pageName, timeoutMs) {
  const models = window["powerbi-client"].models;
  const el = document.getElementById("r");
  window.powerbi.reset(el);
  return new Promise((resolve) => {
    const r = window.powerbi.embed(el, {
      type: "report", id: report, embedUrl: `https://app.powerbi.com/reportEmbed?reportId=${encodeURIComponent(report)}&groupId=${encodeURIComponent(group)}`,
      accessToken: token, tokenType: models.TokenType.Aad, pageName,
      settings: { panes: { filters: { visible: false }, pageNavigation: { visible: false } }, background: models.BackgroundType.Default },
    });
    const timer = setTimeout(() => resolve("timeout"), timeoutMs);
    r.on("rendered", () => { clearTimeout(timer); resolve("rendered"); });
    r.on("error", (e) => { clearTimeout(timer); resolve(`error: ${JSON.stringify(e.detail).slice(0, 400)}`); });
  });
}
/* node:coverage enable */

async function shoot(o) {
  const { puppeteer, token, groupId, reportId, outDir, pages, scriptPath, settleMs = 4000, renderTimeoutMs = 90_000, mkdir = (d) => fs.mkdirSync(d, { recursive: true }) } = o;
  const browser = await puppeteer.launch(launchOptions(o));
  try {
    const page = await browser.newPage();
    const consoleErrors = [];
    page.on("console", (m) => { if (m.type() === "error") consoleErrors.push(m.text()); });
    await page.setContent(PAGE_HTML, { waitUntil: "load" });
    await page.addScriptTag({ path: scriptPath });
    mkdir(outDir);
    const results = [];
    for (const pageName of pages) {
      const status = await page.evaluate(embedAndWait, token, groupId, reportId, pageName, renderTimeoutMs);
      await new Promise((r) => setTimeout(r, settleMs)); // 描画アニメーションの完了待ち
      const file = path.join(outDir, `report-${pageName}.png`);
      await (await page.$("#r")).screenshot({ path: file });
      results.push({ page: pageName, status, file });
    }
    return Object.assign(results, { consoleErrors });
  } finally {
    await browser.close();
  }
}

async function main(argv, env, deps = {}) {
  const d = { loadPuppeteer, resolveBrowser: () => resolveBrowserPath({ env }), resolveScript: () => resolvePowerBiClient(), io: console, ...deps };
  const [groupId, reportId, outDir, ...pages] = argv;
  if (!outDir || pages.length === 0) { d.io.error("usage: shoot_report.js <groupId> <reportId> <outDir> <pageNames...>"); return 2; }
  const token = env.PBI_TOKEN;
  if (!token) { d.io.error("PBI_TOKEN is not set"); return 2; }
  // 読み取ったら環境変数からは消す（process.env を渡された場合、起動するブラウザーやその子プロセスに継承させないため）
  delete env.PBI_TOKEN;
  try {
    const isRoot = typeof process.getuid === "function" && process.getuid() === 0;
    const results = await shoot({ puppeteer: d.loadPuppeteer(env), browserPath: d.resolveBrowser(), scriptPath: d.resolveScript(), token, env, groupId, reportId, outDir, pages, isRoot, settleMs: d.settleMs, mkdir: d.mkdir });
    results.forEach((r) => d.io.log(`${r.page} ${r.status} ${r.file}`));
    if (results.consoleErrors && results.consoleErrors.length) d.io.log(`console errors: ${results.consoleErrors.slice(0, 5).join(" | ").slice(0, 800)}`);
    const failed = results.filter((r) => r.status !== "rendered");
    if (failed.length) d.io.error(`not rendered: ${failed.map((r) => `${r.page}=${r.status}`).join(", ")}`);
    return failed.length === 0 ? 0 : 1;
  } catch (e) {
    d.io.error(`shoot failed: ${e.message}`);
    return 1;
  }
}

if (require.main === module) {
  main(process.argv.slice(2), process.env).then((code) => process.exit(code));
}

module.exports = { PAGE_HTML, resolveBrowserPath, launchOptions, resolvePowerBiClient, loadPuppeteer, shoot, main };
