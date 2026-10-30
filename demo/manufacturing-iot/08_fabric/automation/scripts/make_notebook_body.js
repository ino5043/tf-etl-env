// Notebook 作成・更新 API のリクエスト本文を作る。.ipynb はそのまま、.py は 1 セルの Notebook に変換する。
// 既定の Lakehouse をメタデータで紐づける。
// Usage: node make_notebook_body.js <source.ipynb|source.py> <displayName> <workspaceId> <lakehouseId> <lakehouseName> <out.json>
"use strict";
const fs = require("fs");
const path = require("path");

function toNotebook(source, ext) {
  if (ext === ".ipynb") return JSON.parse(source);
  if (ext === ".py") {
    return {
      nbformat: 4, nbformat_minor: 5,
      metadata: { kernelspec: { display_name: "PySpark", language: "python", name: "synapse_pyspark" }, language_info: { name: "python" } },
      cells: [{ cell_type: "code", metadata: {}, execution_count: null, outputs: [], source: source.split(/(?<=\n)/) }],
    };
  }
  throw new Error(`unsupported notebook source type: ${ext}（.ipynb か .py）`);
}

function buildNotebookBody({ source, ext, name, workspaceId, lakehouseId, lakehouseName }) {
  const nb = toNotebook(source, ext);
  const metadata = nb.metadata || {};
  const lakehouse = { default_lakehouse: lakehouseId, default_lakehouse_name: lakehouseName, default_lakehouse_workspace_id: workspaceId };
  const withLakehouse = { ...nb, metadata: { ...metadata, dependencies: { ...(metadata.dependencies || {}), lakehouse } } };
  return {
    displayName: name,
    definition: {
      format: "ipynb",
      parts: [{ path: "notebook-content.ipynb", payload: Buffer.from(JSON.stringify(withLakehouse), "utf8").toString("base64"), payloadType: "InlineBase64" }],
    },
  };
}

const buildNotebookBodyFromFile = (file, opts) => buildNotebookBody({ ...opts, source: fs.readFileSync(file, "utf8"), ext: path.extname(file) });

if (require.main === module) {
  try {
    const [src, name, workspaceId, lakehouseId, lakehouseName, out] = process.argv.slice(2);
    if (!out) throw new Error("usage: make_notebook_body.js <src> <name> <ws> <lh> <lhName> <out>");
    fs.writeFileSync(out, JSON.stringify(buildNotebookBodyFromFile(src, { name, workspaceId, lakehouseId, lakehouseName })));
  } catch (e) { console.error(`make_notebook_body failed: ${e.message}`); process.exit(1); }
}

module.exports = { buildNotebookBody, buildNotebookBodyFromFile };
