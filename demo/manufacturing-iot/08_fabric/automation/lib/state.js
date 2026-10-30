// 作成した項目の ID を WORK_DIR/state.json に保存する。状態は凍結オブジェクトで、更新は常に新しいオブジェクトを返す
"use strict";
const fs = require("node:fs");
const path = require("node:path");
const { DeployError } = require("./errors");

const STATE_FILE = "state.json";
const LEGACY_FILE = "state.env"; // bash 版の状態ファイル（初回だけ読み込んで移行する）
const KEY_RE = /^[A-Z][A-Z0-9_]*$/;

function parseLegacyEnv(text) {
  return Object.fromEntries(text.split(/\r?\n/).map((l) => l.match(/^([A-Z][A-Z0-9_]*)=(.*)$/)).filter(Boolean).map((m) => [m[1], m[2]]));
}

function updateState(state, patch) {
  for (const [k, v] of Object.entries(patch)) {
    if (!KEY_RE.test(k)) throw new DeployError(`invalid state key: ${k}`, { code: "STATE" });
    if (typeof v !== "string" || v === "") throw new DeployError(`state value for ${k} must be a non-empty string`, { code: "STATE" });
  }
  return Object.freeze({ ...state, ...patch });
}

function loadState(workDir) {
  const file = path.join(workDir, STATE_FILE);
  if (fs.existsSync(file)) {
    try {
      return updateState({}, JSON.parse(fs.readFileSync(file, "utf8")));
    } catch (err) {
      throw new DeployError(`cannot read ${file}: ${err.message}（内容を確認するか、削除して最初から実行する）`, { code: "STATE", cause: err });
    }
  }
  const legacy = path.join(workDir, LEGACY_FILE);
  return fs.existsSync(legacy) ? updateState({}, parseLegacyEnv(fs.readFileSync(legacy, "utf8"))) : Object.freeze({});
}

function saveState(workDir, state) {
  fs.mkdirSync(workDir, { recursive: true });
  const file = path.join(workDir, STATE_FILE);
  const tmp = `${file}.${process.pid}.tmp`;
  fs.writeFileSync(tmp, `${JSON.stringify(state, null, 2)}\n`);
  fs.renameSync(tmp, file);
}

// state.json が別のワークスペース用に作られたものでないかを確認する。
// 戻り値: "empty"（状態なし）/ "match"（一致）/ "unverified"（名前が未記録の古い状態）。名前が違えば止める
function checkWorkspaceState(state, workspaceName) {
  if (!state.WORKSPACE_NAME) return Object.keys(state).length === 0 ? "empty" : "unverified";
  if (state.WORKSPACE_NAME === workspaceName) return "match";
  throw new DeployError(
    `state.json は別のワークスペース（${state.WORKSPACE_NAME}）のもの。WORKSPACE_NAME=${workspaceName} で実行するなら別の WORK_DIR を指定するか、state.json を削除する`,
    { code: "STATE_MISMATCH" },
  );
}

// 手順の途中でも ID を保存できるように、読み出しと確定（保存）だけを公開する
function createStateStore(workDir) {
  let current = loadState(workDir);
  return Object.freeze({
    get: () => current,
    commit(patch) {
      const next = updateState(current, patch);
      saveState(workDir, next);
      current = next;
      return next;
    },
  });
}

module.exports = { loadState, saveState, updateState, createStateStore, parseLegacyEnv, checkWorkspaceState, STATE_FILE };
