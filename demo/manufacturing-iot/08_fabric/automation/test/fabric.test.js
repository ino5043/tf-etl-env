"use strict";
const { test, describe } = require("node:test");
const assert = require("node:assert/strict");
const { createFabricClient } = require("../lib/fabric");
const { createOneLakeClient } = require("../lib/onelake");
const { createApiClient } = require("../lib/api");
const { createHttpClient } = require("../lib/http");
const { HttpError, TimeoutError, DeployError } = require("../lib/errors");
const { mockFetch, jsonResponse, textResponse, recordingClock, collectingLogger, fakeTokens } = require("./helpers");

const F = "https://api.fabric.microsoft.com/v1";
const OP = `${F}/operations/op1`;

function setup(routes, opts = {}) {
  const { fetchImpl, calls } = mockFetch(routes);
  const clock = recordingClock();
  const logger = collectingLogger();
  const http = createHttpClient({ fetchImpl, sleep: clock.sleep, now: clock.now, random: () => 1, baseDelayMs: 1, maxDelayMs: 1 });
  const api = createApiClient({ http, tokens: fakeTokens() });
  const fabric = createFabricClient({ api, logger, sleep: clock.sleep, now: clock.now, lroPollMs: 1000, lroBudgetMs: 10_000, jobPollMs: 5000, ...opts });
  return { fabric, onelake: createOneLakeClient({ api }), calls, clock, logger };
}

describe("listAll (pagination)", () => {
  test("follows continuationToken until exhausted", async () => {
    const { fabric, calls } = setup([["GET", /\/workspaces\/w\/items/, [
      () => jsonResponse(200, { value: [{ id: "1" }], continuationToken: "abc+/=", continuationUri: `${F}/workspaces/w/items?type=Notebook&continuationToken=abc` }),
      () => jsonResponse(200, { value: [{ id: "2" }] }),
    ]]]);
    const all = await fabric.listAll("/workspaces/w/items?type=Notebook");
    assert.deepEqual(all.map((x) => x.id), ["1", "2"]);
    assert.equal(calls[1].url, `${F}/workspaces/w/items?type=Notebook&continuationToken=abc%2B%2F%3D`);
  });

  test("stops on a repeated token instead of looping forever", async () => {
    const { fabric } = setup([["GET", /items/, () => jsonResponse(200, { value: [], continuationToken: "same" })]]);
    await assert.rejects(fabric.listAll("/workspaces/w/items"), /pagination/);
  });

  test("findItemId matches the exact display name across pages", async () => {
    const { fabric, calls } = setup([["GET", /items/, [
      () => jsonResponse(200, { value: [{ id: "a", displayName: "fabric_pipeline_old" }], continuationToken: "t" }),
      () => jsonResponse(200, { value: [{ id: "b", displayName: "fabric_pipeline" }] }),
    ]]]);
    assert.equal(await fabric.findItemId("w", "Notebook", "fabric_pipeline"), "b");
    assert.match(calls[0].url, /items\?type=Notebook$/);
  });
});

describe("waitOperation", () => {
  test("polls until Succeeded and returns /result", async () => {
    const { fabric, calls, clock } = setup([
      ["GET", /operations\/op1$/, [() => jsonResponse(200, { status: "Running" }, { "Retry-After": "2" }), () => jsonResponse(200, { status: "Succeeded" })]],
      ["GET", /operations\/op1\/result$/, () => jsonResponse(200, { id: "item1" })],
    ]);
    assert.deepEqual(await fabric.waitOperation(OP, { withResult: true }), { id: "item1" });
    assert.equal(calls.length, 3);
    assert.deepEqual(clock.sleeps, [1000, 2000]); // 初回は既定間隔、以降は Retry-After
  });

  test("without withResult does not call /result", async () => {
    const { fabric, calls } = setup([["GET", /op1$/, () => jsonResponse(200, { status: "Succeeded" })]]);
    assert.equal(await fabric.waitOperation(OP), null);
    assert.equal(calls.length, 1);
  });

  test("Failed operation throws with the error detail", async () => {
    const { fabric } = setup([["GET", /op1$/, () => jsonResponse(200, { status: "Failed", error: { errorCode: "X", message: "definition invalid" } })]]);
    await assert.rejects(fabric.waitOperation(OP), (e) => e.code === "OPERATION_FAILED" && /definition invalid/.test(e.message));
  });

  test("respects the time budget", async () => {
    const { fabric } = setup([["GET", /op1$/, () => jsonResponse(200, { status: "Running" })]]);
    await assert.rejects(fabric.waitOperation(OP, { budgetMs: 3500 }), (e) => e instanceof TimeoutError && /3500/.test(e.message));
  });

  test("refuses a Location on another host (token is never sent)", async () => {
    const { fabric, calls } = setup([["GET", /./, () => jsonResponse(200, { status: "Succeeded" })]]);
    await assert.rejects(fabric.waitOperation("https://evil.example.com/v1/operations/1"), (e) => e.code === "UNTRUSTED_URL");
    await assert.rejects(fabric.waitOperation("http://api.fabric.microsoft.com/v1/operations/1"), (e) => e.code === "UNTRUSTED_URL");
    await assert.rejects(fabric.waitOperation("https://onelake.dfs.fabric.microsoft.com/x"), (e) => e.code === "UNTRUSTED_URL");
    assert.equal(calls.length, 0);
  });

  test("missing Location is an error, not a silent success", async () => {
    const { fabric } = setup([]);
    await assert.rejects(fabric.waitOperation(undefined), /Location/);
  });
});

describe("createItem / createOrUpdateItem", () => {
  const body = { displayName: "nb", definition: { parts: [] } };

  test("201: returns the created item synchronously", async () => {
    const { fabric } = setup([["POST", /\/workspaces\/w\/notebooks$/, () => jsonResponse(201, { id: "n1" })]]);
    assert.deepEqual(await fabric.createItem("/workspaces/w/notebooks", body), { id: "n1" });
  });

  test("202: follows the long-running operation and returns its result", async () => {
    const { fabric } = setup([
      ["POST", /notebooks$/, () => jsonResponse(202, null, { Location: OP, "Retry-After": "1" })],
      ["GET", /op1$/, () => jsonResponse(200, { status: "Succeeded" })],
      ["GET", /op1\/result$/, () => jsonResponse(200, { id: "n2" })],
    ]);
    assert.deepEqual(await fabric.createItem("/workspaces/w/notebooks", body), { id: "n2" });
  });

  test("error statuses throw HttpError with the body", async () => {
    const { fabric } = setup([["POST", /notebooks$/, () => jsonResponse(409, { errorCode: "ItemDisplayNameAlreadyInUse" })]]);
    await assert.rejects(fabric.createItem("/workspaces/w/notebooks", body), (e) => e instanceof HttpError && e.status === 409 && /AlreadyInUse/.test(e.bodyExcerpt));
  });

  test("201 without id is rejected", async () => {
    const { fabric } = setup([["POST", /notebooks$/, () => jsonResponse(201, {})]]);
    await assert.rejects(fabric.createItem("/workspaces/w/notebooks", body), /no id/);
  });

  test("new item: lists by type then creates", async () => {
    const { fabric, calls } = setup([
      ["GET", /items\?type=SemanticModel/, () => jsonResponse(200, { value: [] })],
      ["POST", /semanticModels$/, () => jsonResponse(201, { id: "m1" })],
    ]);
    assert.deepEqual(await fabric.createOrUpdateItem({ workspaceId: "w", type: "SemanticModel", body }), { id: "m1", created: true });
    assert.deepEqual(calls[1].json(), body);
  });

  test("existing item: sends only the definition to updateDefinition (200)", async () => {
    const { fabric, calls } = setup([
      ["GET", /items\?type=Report/, () => jsonResponse(200, { value: [{ id: "r1", displayName: "nb" }] })],
      ["POST", /reports\/r1\/updateDefinition$/, () => jsonResponse(200, {})],
    ]);
    assert.deepEqual(await fabric.createOrUpdateItem({ workspaceId: "w", type: "Report", body }), { id: "r1", created: false });
    assert.deepEqual(calls[1].json(), { definition: body.definition });
  });

  test("existing item: 202 updateDefinition waits for the operation", async () => {
    const { fabric, calls } = setup([
      ["GET", /items\?type=DataPipeline/, () => jsonResponse(200, { value: [{ id: "p1", displayName: "nb" }] })],
      ["POST", /dataPipelines\/p1\/updateDefinition$/, () => jsonResponse(202, null, { Location: OP })],
      ["GET", /op1$/, () => jsonResponse(200, { status: "Succeeded" })],
    ]);
    await fabric.createOrUpdateItem({ workspaceId: "w", type: "DataPipeline", body });
    assert.equal(calls.length, 3);
  });

  test("existing item: 400 from updateDefinition fails the step (old definition must not be reported as success)", async () => {
    const { fabric } = setup([
      ["GET", /items/, () => jsonResponse(200, { value: [{ id: "n1", displayName: "nb" }] })],
      ["POST", /updateDefinition$/, () => jsonResponse(400, { errorCode: "InvalidDefinitionParts" })],
    ]);
    await assert.rejects(fabric.createOrUpdateItem({ workspaceId: "w", type: "Notebook", body }), (e) => e.status === 400);
  });

  test("unknown item type is rejected", async () => {
    const { fabric } = setup([]);
    await assert.rejects(fabric.createOrUpdateItem({ workspaceId: "w", type: "Dashboard", body }), /item type/);
  });
});

describe("ensureWorkspace / ensureLakehouse / getLakehouse / refreshSqlEndpointMetadata", () => {
  test("workspace: reuses an existing one by name", async () => {
    const { fabric, calls } = setup([["GET", /\/workspaces$/, () => jsonResponse(200, { value: [{ id: "w1", displayName: "demo" }] })]]);
    assert.deepEqual(await fabric.ensureWorkspace({ name: "demo", capacityId: "c" }), { id: "w1", created: false });
    assert.equal(calls.length, 1);
  });

  test("workspace: creates with capacity (JSON-safe name)", async () => {
    const { fabric, calls } = setup([
      ["GET", /\/workspaces$/, () => jsonResponse(200, { value: [] })],
      ["POST", /\/workspaces$/, () => jsonResponse(201, { id: "w2" })],
    ]);
    assert.deepEqual(await fabric.ensureWorkspace({ name: 'demo "x"', capacityId: "c", description: "d" }), { id: "w2", created: true });
    assert.deepEqual(calls[1].json(), { displayName: 'demo "x"', description: "d", capacityId: "c" });
  });

  test("lakehouse: creates with schemas enabled", async () => {
    const { fabric, calls } = setup([
      ["GET", /items\?type=Lakehouse/, () => jsonResponse(200, { value: [] })],
      ["POST", /lakehouses$/, () => jsonResponse(201, { id: "l1" })],
    ]);
    assert.equal((await fabric.ensureLakehouse({ workspaceId: "w", name: "lh" })).id, "l1");
    assert.deepEqual(calls[1].json(), { displayName: "lh", creationPayload: { enableSchemas: true } });
  });

  test("getLakehouse returns the body", async () => {
    const { fabric } = setup([["GET", /lakehouses\/l1$/, () => jsonResponse(200, { id: "l1", properties: {} })]]);
    assert.equal((await fabric.getLakehouse("w", "l1")).id, "l1");
  });

  test("refreshSqlEndpointMetadata waits for 202 and only warns on failure", async () => {
    const ok = setup([
      ["POST", /sqlEndpoints\/s1\/refreshMetadata/, () => jsonResponse(202, null, { Location: OP })],
      ["GET", /op1$/, () => jsonResponse(200, { status: "Succeeded" })],
    ]);
    assert.equal(await ok.fabric.refreshSqlEndpointMetadata("w", "s1"), true);
    const ng = setup([["POST", /refreshMetadata/, () => jsonResponse(404, { errorCode: "NotFound" })]]);
    assert.equal(await ng.fabric.refreshSqlEndpointMetadata("w", "s1"), false);
    assert.ok(ng.logger.lines.some((l) => l.level === "warn" && l.event === "sql_endpoint_refresh_failed"));
  });
});

describe("runJob", () => {
  const JOB = `${F}/workspaces/w/items/n1/jobs/instances/j1`;
  const start = (status = 202, headers = { Location: JOB }) => ["POST", /jobs\/instances\?jobType=RunNotebook$/, () => jsonResponse(status, null, headers)];

  test("starts the job and polls until Completed", async () => {
    const { fabric, calls, logger } = setup([start(), ["GET", /instances\/j1$/, [() => jsonResponse(200, { status: "NotStarted" }), () => jsonResponse(200, { status: "InProgress" }), () => jsonResponse(200, { status: "Completed" })]]]);
    const job = await fabric.runJob({ workspaceId: "w", itemId: "n1", jobType: "RunNotebook", body: { a: 1 }, timeoutMs: 60_000 });
    assert.equal(job.status, "Completed");
    assert.deepEqual(calls[0].json(), { a: 1 });
    assert.ok(logger.lines.some((l) => l.event === "job_status" && l.status === "InProgress"));
  });

  test("Failed job throws with failureReason", async () => {
    const { fabric } = setup([start(), ["GET", /j1$/, () => jsonResponse(200, { status: "Failed", failureReason: { message: "Spark died" } })]]);
    await assert.rejects(fabric.runJob({ workspaceId: "w", itemId: "n1", jobType: "RunNotebook" }), (e) => e.code === "JOB_FAILED" && /Spark died/.test(e.message));
  });

  test("Cancelled / Deduped also fail", async () => {
    for (const st of ["Cancelled", "Deduped"]) {
      const { fabric } = setup([start(), ["GET", /j1$/, () => jsonResponse(200, { status: st })]]);
      await assert.rejects(fabric.runJob({ workspaceId: "w", itemId: "n1", jobType: "RunNotebook" }), new RegExp(st));
    }
  });

  test("times out", async () => {
    const { fabric } = setup([start(), ["GET", /j1$/, () => jsonResponse(200, { status: "InProgress" })]]);
    await assert.rejects(fabric.runJob({ workspaceId: "w", itemId: "n1", jobType: "RunNotebook", timeoutMs: 12_000 }), TimeoutError);
  });

  test("non-202 start fails with status", async () => {
    const { fabric } = setup([start(400, {})]);
    await assert.rejects(fabric.runJob({ workspaceId: "w", itemId: "n1", jobType: "RunNotebook" }), (e) => e.status === 400);
  });

  test("202 without Location fails instead of polling an empty URL", async () => {
    const { fabric } = setup([start(202, {})]);
    await assert.rejects(fabric.runJob({ workspaceId: "w", itemId: "n1", jobType: "RunNotebook" }), /Location/);
  });

  test("rejects an unknown job type and a foreign Location host", async () => {
    const { fabric } = setup([start(202, { Location: "https://evil.example.com/j" })]);
    await assert.rejects(fabric.runJob({ workspaceId: "w", itemId: "n1", jobType: "Rm -rf" }), /job type/);
    await assert.rejects(fabric.runJob({ workspaceId: "w", itemId: "n1", jobType: "RunNotebook" }), (e) => e instanceof DeployError && e.code === "UNTRUSTED_URL");
  });
});

describe("OneLake", () => {
  const BASE = "https://onelake.dfs.fabric.microsoft.com/w/l/Files/sample_data/a%20b.csv";

  test("upload: create, append, flush with the right positions", async () => {
    const { onelake, calls } = setup([
      ["PUT", /resource=file$/, () => textResponse(201, "")],
      ["PATCH", /action=append&position=0$/, () => textResponse(202, "")],
      ["PATCH", /action=flush&position=4$/, () => textResponse(200, "")],
    ]);
    await onelake.uploadFile({ workspaceId: "w", lakehouseId: "l", remotePath: "sample_data/a b.csv", content: Buffer.from("x,y\n") });
    assert.deepEqual(calls.map((c) => `${c.method} ${c.url}`), [`PUT ${BASE}?resource=file`, `PATCH ${BASE}?action=append&position=0`, `PATCH ${BASE}?action=flush&position=4`]);
    assert.equal(calls[1].body, "x,y\n");
    assert.equal(calls[0].headers["x-ms-version"], "2023-11-03");
    assert.equal(calls[0].headers.authorization, "Bearer tok-for-https://storage.azure.com");
  });

  test("upload fails on an unexpected status", async () => {
    const { onelake } = setup([["PUT", /resource=file$/, () => textResponse(403, "AuthorizationPermissionMismatch")]]);
    await assert.rejects(onelake.uploadFile({ workspaceId: "w", lakehouseId: "l", remotePath: "a.csv", content: Buffer.from("x") }), (e) => e.status === 403);
  });

  test("rejects path traversal", async () => {
    const { onelake } = setup([]);
    await assert.rejects(onelake.uploadFile({ workspaceId: "w", lakehouseId: "l", remotePath: "../x", content: Buffer.from("x") }), /path/);
  });

  test("downloadText returns the text; 404 fails", async () => {
    const ok = setup([["GET", /_verify\/v\.json$/, () => textResponse(200, '{"a":1}', { "content-type": "application/octet-stream" })]]);
    assert.equal(await ok.onelake.downloadText({ workspaceId: "w", lakehouseId: "l", remotePath: "_verify/v.json" }), '{"a":1}');
    const ng = setup([["GET", /./, () => textResponse(404, "PathNotFound")]]);
    await assert.rejects(ng.onelake.downloadText({ workspaceId: "w", lakehouseId: "l", remotePath: "x" }), (e) => e.status === 404);
  });
});
