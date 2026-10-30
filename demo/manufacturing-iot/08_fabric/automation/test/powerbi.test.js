"use strict";
const { test, describe } = require("node:test");
const assert = require("node:assert/strict");
const { createPowerBiClient, extractRefreshRequestId } = require("../lib/powerbi");
const { createApiClient } = require("../lib/api");
const { createHttpClient } = require("../lib/http");
const { TimeoutError } = require("../lib/errors");
const { mockFetch, jsonResponse, recordingClock, collectingLogger, fakeTokens } = require("./helpers");

const REFRESHES = /\/groups\/w\/datasets\/d\/refreshes/;

function setup(routes) {
  const { fetchImpl, calls } = mockFetch(routes);
  const clock = recordingClock();
  const http = createHttpClient({ fetchImpl, sleep: clock.sleep, now: clock.now, random: () => 1, baseDelayMs: 1, maxDelayMs: 1 });
  const pbi = createPowerBiClient({ api: createApiClient({ http, tokens: fakeTokens() }), logger: collectingLogger(), sleep: clock.sleep, now: clock.now, pollMs: 5000, budgetMs: 60_000 });
  return { pbi, calls, clock };
}

describe("extractRefreshRequestId", () => {
  test("prefers the Location /refreshes/{id}, then RequestId, then the generic x-ms-request-id", () => {
    const loc = "https://api.powerbi.com/v1.0/myorg/groups/w/datasets/d/refreshes/c-1";
    assert.equal(extractRefreshRequestId({ location: loc, requestid: "b", "x-ms-request-id": "a" }), "c-1");
    assert.equal(extractRefreshRequestId({ location: `${loc}?x=1`, "x-ms-request-id": "a" }), "c-1");
    assert.equal(extractRefreshRequestId({ requestid: "b", "x-ms-request-id": "a" }), "b");
    assert.equal(extractRefreshRequestId({ "x-ms-request-id": "a" }), "a");
    assert.equal(extractRefreshRequestId({ location: "https://api.powerbi.com/v1.0/myorg/other/1", "x-ms-request-id": "a" }), "a");
    assert.equal(extractRefreshRequestId({}), null);
  });
});

describe("refreshDataset", () => {
  test("matches the refresh by request id and ignores the previous run", async () => {
    const { pbi, calls } = setup([
      ["POST", REFRESHES, () => jsonResponse(202, null, { "x-ms-request-id": "new" })],
      ["GET", REFRESHES, [
        () => jsonResponse(200, { value: [{ requestId: "old", status: "Completed" }] }), // 新しい更新がまだ一覧に出ていない
        () => jsonResponse(200, { value: [{ requestId: "new", status: "Unknown" }, { requestId: "old", status: "Completed" }] }),
        () => jsonResponse(200, { value: [{ requestId: "new", status: "Completed" }, { requestId: "old", status: "Completed" }] }),
      ]],
    ]);
    const r = await pbi.refreshDataset("w", "d");
    assert.equal(r.status, "Completed");
    assert.equal(r.requestId, "new");
    assert.deepEqual(calls[0].json(), { type: "full" });
    assert.match(calls[1].url, /\$top=/);
    assert.equal(calls.length, 4);
  });

  test("an old Failed run does not fail the new one", async () => {
    const { pbi } = setup([
      ["POST", REFRESHES, () => jsonResponse(202, null, { RequestId: "n" })],
      ["GET", REFRESHES, [() => jsonResponse(200, { value: [{ requestId: "x", status: "Failed" }] }), () => jsonResponse(200, { value: [{ requestId: "n", status: "Completed" }] })]],
    ]);
    assert.equal((await pbi.refreshDataset("w", "d")).status, "Completed");
  });

  test("Failed refresh throws with serviceExceptionJson", async () => {
    const { pbi } = setup([
      ["POST", REFRESHES, () => jsonResponse(202, null, { "x-ms-request-id": "n" })],
      ["GET", REFRESHES, () => jsonResponse(200, { value: [{ requestId: "n", status: "Failed", serviceExceptionJson: '{"errorCode":"DeltaTableNotFound"}' }] })],
    ]);
    await assert.rejects(pbi.refreshDataset("w", "d"), (e) => e.code === "REFRESH_FAILED" && /DeltaTableNotFound/.test(e.message));
  });

  test("POST error status fails (not silently ignored)", async () => {
    const { pbi } = setup([["POST", REFRESHES, () => jsonResponse(400, { error: { code: "InvalidRequest" } })]]);
    await assert.rejects(pbi.refreshDataset("w", "d"), (e) => e.status === 400);
  });

  test("202 without any request id fails", async () => {
    const { pbi } = setup([["POST", REFRESHES, () => jsonResponse(202, null)]]);
    await assert.rejects(pbi.refreshDataset("w", "d"), /request id/);
  });

  test("times out within the budget", async () => {
    const { pbi } = setup([
      ["POST", REFRESHES, () => jsonResponse(202, null, { "x-ms-request-id": "n" })],
      ["GET", REFRESHES, () => jsonResponse(200, { value: [{ requestId: "n", status: "Unknown" }] })],
    ]);
    await assert.rejects(pbi.refreshDataset("w", "d"), TimeoutError);
  });
});

describe("executeQuery", () => {
  test("posts the DAX query and returns the JSON (retries 5xx because the query is read-only)", async () => {
    const { pbi, calls } = setup([["POST", /executeQueries$/, [() => jsonResponse(503, {}), () => jsonResponse(200, { results: [{ tables: [] }] })]]]);
    assert.deepEqual(await pbi.executeQuery("w", "d", "EVALUATE ROW(\"v\", 1)"), { results: [{ tables: [] }] });
    assert.deepEqual(calls[1].json(), { queries: [{ query: "EVALUATE ROW(\"v\", 1)" }], serializerSettings: { includeNulls: true } });
  });

  test("400 with an error body is returned (the caller reports the measure error)", async () => {
    const { pbi } = setup([["POST", /executeQueries$/, () => jsonResponse(400, { error: { code: "DatasetExecuteQueriesError", pbi_error: { details: [{ detail: { value: "bad" } }] } } })]]);
    const r = await pbi.executeQuery("w", "d", "EVALUATE x");
    assert.equal(r.error.code, "DatasetExecuteQueriesError");
  });

  test("401 is still an error", async () => {
    const { pbi } = setup([["POST", /executeQueries$/, () => jsonResponse(401, {})]]);
    await assert.rejects(pbi.executeQuery("w", "d", "EVALUATE x"), (e) => e.status === 401);
  });
});
