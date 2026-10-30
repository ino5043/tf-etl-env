// OneLake（ADLS Gen2 互換の DFS API）で Lakehouse の Files にアップロード・ダウンロードする
"use strict";
const { ONELAKE } = require("./endpoints");
const { DeployError } = require("./errors");

const VERSION_HEADER = Object.freeze({ "x-ms-version": "2023-11-03" });

function fileUrl(workspaceId, lakehouseId, remotePath) {
  const segments = String(remotePath).split("/");
  if (segments.some((s) => s === "" || s === "." || s === "..")) throw new DeployError(`invalid OneLake path: ${remotePath}`, { code: "ONELAKE_PATH" });
  return `${ONELAKE}/${encodeURIComponent(workspaceId)}/${encodeURIComponent(lakehouseId)}/Files/${segments.map(encodeURIComponent).join("/")}`;
}

function createOneLakeClient({ api }) {
  // 作成（空ファイル）→ 追記 → 確定（flush）。どれも同じ内容で再実行しても結果は同じなので、5xx・通信エラーでも再送してよい
  async function uploadFile({ workspaceId, lakehouseId, remotePath, content }) {
    const url = fileUrl(workspaceId, lakehouseId, remotePath);
    const empty = Buffer.alloc(0); // Content-Length: 0 を送るため、本文なしではなく空の本文にする
    await api.request("PUT", `${url}?resource=file`, { headers: VERSION_HEADER, body: empty, expectStatus: [201] });
    await api.request("PATCH", `${url}?action=append&position=0`, { headers: { ...VERSION_HEADER, "Content-Type": "application/octet-stream" }, body: content, expectStatus: [202], idempotent: true });
    await api.request("PATCH", `${url}?action=flush&position=${content.length}`, { headers: VERSION_HEADER, body: empty, expectStatus: [200], idempotent: true });
  }

  async function downloadText({ workspaceId, lakehouseId, remotePath }) {
    return (await api.request("GET", fileUrl(workspaceId, lakehouseId, remotePath), { headers: VERSION_HEADER, expectStatus: [200] })).text;
  }

  return Object.freeze({ uploadFile, downloadText });
}

module.exports = { createOneLakeClient, fileUrl };
