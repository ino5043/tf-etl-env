// config.sh の読み込みと検証。config.sh はシェルとして実行せず、KEY="value" / KEY='value' / KEY=value（先頭の export 可）の行だけを解釈する。
// 優先順位: 既定値 < config.sh < 環境変数
"use strict";
const fs = require("node:fs");
const path = require("node:path");
const { ConfigError } = require("./errors");

const DEFAULTS = Object.freeze({
  WORKSPACE_NAME: "factory-etl-demo",
  LAKEHOUSE_NAME: "factory_lakehouse",
  MODEL_NAME: "工場生産ダッシュボード",
  REPORT_NAME: "工場長 朝会レポート",
  PIPELINE_NAME: "pl_daily_production_load",
  TARGET_DATE: "2026-10-30",
  DEMO_FROM: "2026-10-01",
  DEMO_TO: "2026-10-31",
});
const KNOWN_KEYS = Object.freeze(["CAPACITY_ID", ...Object.keys(DEFAULTS), "REQUESTS_CA_BUNDLE", "WORK_DIR"]);
const GUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const LAKEHOUSE_RE = /^[A-Za-z][A-Za-z0-9_]{0,255}$/;
const LINE_RE = /^(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)=(.*)$/;

// エスケープされていない $ または ` があるか（\$ と \` は文字そのもの）
function hasUnescapedExpansion(s) {
  for (let i = 0; i < s.length; i += 1) {
    if (s[i] === "\\" && /[$`"\\]/.test(s[i + 1] || "")) i += 1;
    else if (s[i] === "$" || s[i] === "`") return true;
  }
  return false;
}

function parseValue(rest, lineNo) {
  const fail = (why) => { throw new ConfigError(`config line ${lineNo}: ${why}（config.sh は実行されない。KEY="value" 形式で書く）`); };
  let value;
  let tail;
  if (rest.startsWith('"')) {
    const m = rest.match(/^"((?:[^"\\]|\\.)*)"(.*)$/);
    if (!m) fail("closing quote missing");
    if (hasUnescapedExpansion(m[1])) fail("$ や ` による展開には対応していない");
    // bash と同じく、" の中で \ がエスケープになるのは $ ` " \ の前だけ。それ以外の \ はそのまま残す（C:\work など）
    value = m[1].replace(/\\([$`"\\])/g, "$1");
    tail = m[2];
  } else if (rest.startsWith("'")) {
    const m = rest.match(/^'([^']*)'(.*)$/);
    if (!m) fail("closing quote missing");
    [, value, tail] = m;
  } else {
    const m = rest.match(/^([^\s#]*)(.*)$/);
    if (/[$`"'();&|<>\\]/.test(m[1])) fail("unsupported characters in unquoted value");
    [, value, tail] = m;
  }
  // 値の後ろに書けるのは空白と「空白 + # コメント」だけ
  if (tail !== "" && !/^\s+(#.*)?$/.test(tail)) fail("unexpected text after the value");
  return value;
}

function parseConfigText(text) {
  const out = {};
  text.split(/\n/).forEach((raw, i) => {
    const line = raw.replace(/\r$/, "").trim();
    if (line === "" || line.startsWith("#")) return;
    const m = line.match(LINE_RE);
    if (!m) throw new ConfigError(`config line ${i + 1}: unsupported syntax（KEY="value" の行とコメントだけが書ける）`);
    out[m[1]] = parseValue(m[2], i + 1);
  });
  return Object.freeze(out);
}

function isIsoDate(s) {
  if (typeof s !== "string" || !/^\d{4}-\d{2}-\d{2}$/.test(s)) return false;
  const [y, m, d] = s.split("-").map(Number);
  const dt = new Date(Date.UTC(y, m - 1, d));
  return dt.getUTCFullYear() === y && dt.getUTCMonth() === m - 1 && dt.getUTCDate() === d;
}

const badName = (v) => typeof v !== "string" || v.trim() === "" || v.length > 256 || /[\u0000-\u001f\u007f]/.test(v);

function collectProblems(v) {
  const problems = [];
  if (!GUID_RE.test(v.CAPACITY_ID || "")) problems.push("CAPACITY_ID は容量の GUID を設定する（必須）");
  for (const k of ["WORKSPACE_NAME", "MODEL_NAME", "REPORT_NAME", "PIPELINE_NAME"]) if (badName(v[k])) problems.push(`${k} は 1〜256 文字（制御文字なし）`);
  if (!LAKEHOUSE_RE.test(v.LAKEHOUSE_NAME || "")) problems.push("LAKEHOUSE_NAME は英字で始まる英数字と _ のみ");
  for (const k of ["TARGET_DATE", "DEMO_FROM", "DEMO_TO"]) if (!isIsoDate(v[k])) problems.push(`${k} は YYYY-MM-DD の実在する日付`);
  if (isIsoDate(v.DEMO_FROM) && isIsoDate(v.DEMO_TO) && v.DEMO_FROM >= v.DEMO_TO) problems.push("DEMO_FROM は DEMO_TO より前の日付");
  return problems;
}

function validateConfig(raw, { defaultWorkDir, cwd = process.cwd() } = {}) {
  const v = { ...DEFAULTS, ...Object.fromEntries(Object.entries(raw).filter(([, x]) => x !== undefined && x !== "")) };
  const problems = collectProblems(v);
  if (problems.length) throw new ConfigError(`invalid config:\n  - ${problems.join("\n  - ")}`, { details: problems });
  return Object.freeze({
    capacityId: v.CAPACITY_ID,
    workspaceName: v.WORKSPACE_NAME,
    lakehouseName: v.LAKEHOUSE_NAME,
    modelName: v.MODEL_NAME,
    reportName: v.REPORT_NAME,
    pipelineName: v.PIPELINE_NAME,
    targetDate: v.TARGET_DATE,
    demoFrom: v.DEMO_FROM,
    demoTo: v.DEMO_TO,
    caBundle: v.REQUESTS_CA_BUNDLE || null,
    workDir: v.WORK_DIR ? path.resolve(cwd, v.WORK_DIR) : defaultWorkDir,
    unknownKeys: Object.freeze(Object.keys(raw).filter((k) => !KNOWN_KEYS.includes(k))),
  });
}

function loadConfig({ configPath, env = process.env, defaultWorkDir, cwd = process.cwd() }) {
  if (!fs.existsSync(configPath)) throw new ConfigError(`設定ファイルがない: ${configPath}（config.example.sh をコピーして作成する）`);
  const fromFile = parseConfigText(fs.readFileSync(configPath, "utf8"));
  const fromEnv = Object.fromEntries(KNOWN_KEYS.filter((k) => env[k]).map((k) => [k, env[k]]));
  return validateConfig({ ...fromFile, ...fromEnv }, { defaultWorkDir, cwd });
}

module.exports = { parseConfigText, validateConfig, loadConfig, isIsoDate, DEFAULTS, KNOWN_KEYS };
