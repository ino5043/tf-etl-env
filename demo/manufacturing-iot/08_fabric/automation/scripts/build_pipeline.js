// Data Pipeline（deploy-guide.md 手順 7）の作成・定義更新 API 本文を作る。
// ノートブック アクティビティで fabric_pipeline を実行し、処理対象日をパイプラインのパラメーターから TARGET_DATE に渡す。
// Usage: node build_pipeline.js <pipeline_name> <workspace_id> <notebook_id> <default_target_date> <out.json>
"use strict";
const fs = require("fs");
const { isIsoDate } = require("../lib/config");

function buildPipeline({ name, workspaceId, notebookId, targetDate }) {
  if (!isIsoDate(targetDate)) throw new Error(`target date must be YYYY-MM-DD: ${targetDate}`);
  if (!name || !workspaceId || !notebookId) throw new Error("name, workspaceId and notebookId are required");
  const content = {
    properties: {
      description: "Daily load: MES CSV -> Bronze -> Silver -> Gold (fabric_pipeline notebook)",
      parameters: { target_date: { type: "string", defaultValue: targetDate } },
      activities: [{
        name: "Run fabric_pipeline",
        type: "TridentNotebook",
        dependsOn: [],
        policy: { timeout: "0.02:00:00", retry: 1, retryIntervalInSeconds: 120, secureOutput: false, secureInput: false },
        typeProperties: {
          notebookId,
          workspaceId,
          // 本番で前日分を処理する場合は値を次の式にする:
          // @formatDateTime(addDays(convertFromUtc(utcNow(), 'Tokyo Standard Time'), -1), 'yyyy-MM-dd')
          parameters: { TARGET_DATE: { value: { value: "@pipeline().parameters.target_date", type: "Expression" }, type: "string" } },
        },
      }],
    },
  };
  return {
    displayName: name,
    description: "Daily production load (manufacturing IoT demo)",
    definition: { parts: [{ path: "pipeline-content.json", payload: Buffer.from(JSON.stringify(content), "utf8").toString("base64"), payloadType: "InlineBase64" }] },
  };
}

if (require.main === module) {
  try {
    const [name, workspaceId, notebookId, targetDate, out] = process.argv.slice(2);
    if (!out) throw new Error("usage: build_pipeline.js <name> <workspace_id> <notebook_id> <target_date> <out>");
    fs.writeFileSync(out, JSON.stringify(buildPipeline({ name, workspaceId, notebookId, targetDate })));
  } catch (e) { console.error(`build_pipeline failed: ${e.message}`); process.exit(1); }
}

module.exports = { buildPipeline };
