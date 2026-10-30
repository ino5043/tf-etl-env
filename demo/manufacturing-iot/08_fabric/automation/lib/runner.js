// 手順の解決と順次実行。各手順の開始・完了・失敗を構造化ログ（step, status, duration_ms）で出す
"use strict";
const { DeployError } = require("./errors");

const STEP_ORDER = Object.freeze(["workspace", "lakehouse", "upload", "notebook", "run", "verify", "model", "report", "pipeline", "shots"]);

function resolveSteps(args) {
  if (!args || args.length === 0) throw new DeployError(`usage: deploy.js all | <step> [<step> ...]（${STEP_ORDER.join(" ")}）`, { code: "USAGE" });
  if (args.includes("all")) {
    if (args.length > 1) throw new DeployError("'all' は単独で指定する", { code: "USAGE" });
    return [...STEP_ORDER];
  }
  const unknown = args.filter((a) => !STEP_ORDER.includes(a));
  if (unknown.length) throw new DeployError(`unknown step: ${unknown.join(", ")}（${STEP_ORDER.join(" ")}）`, { code: "USAGE" });
  return [...args];
}

function errorFields(e) {
  const f = { error_type: e.name, code: e.code, message: e.message, http_status: e.status, body_excerpt: e.bodyExcerpt, details: e.details };
  return Object.fromEntries(Object.entries(f).filter(([, v]) => v !== undefined));
}

async function runSteps(names, steps, ctx, { now = Date.now } = {}) {
  for (const step of names) {
    const started = now();
    ctx.logger.info("step_started", { step });
    try {
      const fields = (await steps[step](ctx)) || {};
      ctx.logger.info("step_completed", { step, status: "ok", duration_ms: now() - started, ...fields });
    } catch (err) {
      ctx.logger.error("step_failed", { step, status: "failed", duration_ms: now() - started, ...errorFields(err) });
      throw Object.assign(err, { step });
    }
  }
}

module.exports = { STEP_ORDER, resolveSteps, runSteps, errorFields };
