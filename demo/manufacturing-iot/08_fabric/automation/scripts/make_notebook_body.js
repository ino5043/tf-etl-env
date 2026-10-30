// Notebook 作成・更新 API のリクエスト本文を作る。.ipynb はそのまま、.py は 1 セルの Notebook に変換する。
// 既定の Lakehouse をメタデータで紐づける。
// Usage: node make_notebook_body.js <source.ipynb|source.py> <displayName> <workspaceId> <lakehouseId> <lakehouseName> <out.json>
const fs = require("fs");
const path = require("path");

const [SRC, NAME, WS, LH, LH_NAME, OUT] = process.argv.slice(2);
if (!OUT) throw new Error("usage: make_notebook_body.js <src> <name> <ws> <lh> <lhName> <out>");

const lakehouse = { default_lakehouse: LH, default_lakehouse_name: LH_NAME, default_lakehouse_workspace_id: WS };
let nb;
if (path.extname(SRC) === ".ipynb") {
  nb = JSON.parse(fs.readFileSync(SRC, "utf8"));
} else {
  const src = fs.readFileSync(SRC, "utf8");
  nb = {
    nbformat: 4, nbformat_minor: 5,
    metadata: { kernelspec: { display_name: "PySpark", language: "python", name: "synapse_pyspark" }, language_info: { name: "python" } },
    cells: [{ cell_type: "code", metadata: {}, execution_count: null, outputs: [], source: src.split(/(?<=\n)/) }],
  };
}
nb.metadata = { ...nb.metadata, dependencies: { ...(nb.metadata.dependencies || {}), lakehouse } };

const definition = {
  format: "ipynb",
  parts: [{ path: "notebook-content.ipynb", payload: Buffer.from(JSON.stringify(nb), "utf8").toString("base64"), payloadType: "InlineBase64" }],
};
fs.writeFileSync(OUT, JSON.stringify({ displayName: NAME, definition }));
