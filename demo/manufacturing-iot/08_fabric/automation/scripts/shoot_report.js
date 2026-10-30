// Power BI レポートを JS ライブラリで埋め込み表示し、各ページを PNG で撮影する（ヘッドレス Edge）
// Usage: node shoot_report.js <groupId> <reportId> <outDir> <pageNames...>   (環境変数 PBI_TOKEN に AAD トークン)
const path = require("path");
const fs = require("fs");
// puppeteer-core の場所: 環境変数 PUPPETEER_CORE_PATH > 通常の require > mermaid-cli に同梱のもの（Windows のグローバルインストール）
function loadPuppeteer() {
  const candidates = [process.env.PUPPETEER_CORE_PATH, "puppeteer-core", process.env.APPDATA && path.join(process.env.APPDATA, "npm/node_modules/@mermaid-js/mermaid-cli/node_modules/puppeteer-core")].filter(Boolean);
  for (const c of candidates) { try { return require(c); } catch (e) { /* 次の候補を試す */ } }
  throw new Error("puppeteer-core が見つかりません（npm i puppeteer-core、または PUPPETEER_CORE_PATH を設定）");
}
const puppeteer = loadPuppeteer();
// 使うブラウザー: 環境変数 BROWSER_PATH（未設定時は Windows の Edge）
const BROWSER = process.env.BROWSER_PATH || "C:/Program Files (x86)/Microsoft/Edge/Application/msedge.exe";

const [GROUP, REPORT, OUT, ...PAGES] = process.argv.slice(2);
const TOKEN = process.env.PBI_TOKEN;
if (!TOKEN) throw new Error("PBI_TOKEN is not set");

const html = `<!doctype html><html><head><meta charset="utf-8">
<script src="https://cdn.jsdelivr.net/npm/powerbi-client@2.23.1/dist/powerbi.min.js"></script>
<style>html,body{margin:0;background:#fff}#r{width:1280px;height:720px}</style></head>
<body><div id="r"></div></body></html>`;

(async () => {
  const browser = await puppeteer.launch({
    executablePath: BROWSER,
    headless: true, args: ["--no-sandbox"], defaultViewport: { width: 1280, height: 720, deviceScaleFactor: 2 },
  });
  try {
    const page = await browser.newPage();
    const errors = [];
    page.on("console", (m) => { if (m.type() === "error") errors.push(m.text()); });
    await page.setContent(html, { waitUntil: "networkidle0" });
    fs.mkdirSync(OUT, { recursive: true });
    for (const pageName of PAGES) {
      const status = await page.evaluate(async (token, group, report, pageName) => {
        const models = window["powerbi-client"].models;
        const el = document.getElementById("r");
        window.powerbi.reset(el);
        return await new Promise((resolve) => {
          const r = window.powerbi.embed(el, {
            type: "report", id: report, embedUrl: `https://app.powerbi.com/reportEmbed?reportId=${report}&groupId=${group}`,
            accessToken: token, tokenType: models.TokenType.Aad, pageName,
            settings: { panes: { filters: { visible: false }, pageNavigation: { visible: false } }, background: models.BackgroundType.Default },
          });
          const timer = setTimeout(() => resolve("timeout"), 90000);
          r.on("rendered", () => { clearTimeout(timer); resolve("rendered"); });
          r.on("error", (e) => { clearTimeout(timer); resolve("error: " + JSON.stringify(e.detail).slice(0, 400)); });
        });
      }, TOKEN, GROUP, REPORT, pageName);
      await new Promise((r) => setTimeout(r, 4000)); // 描画アニメーションの完了待ち
      const file = path.join(OUT, `report-${pageName}.png`);
      await (await page.$("#r")).screenshot({ path: file });
      console.log(pageName, status, file);
    }
    if (errors.length) console.log("console errors:", errors.slice(0, 5).join(" | ").slice(0, 800));
  } finally {
    await browser.close();
  }
})().catch((e) => { console.error("shoot failed:", e.message); process.exit(1); });
