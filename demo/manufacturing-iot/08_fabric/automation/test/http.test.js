"use strict";
const { test, describe } = require("node:test");
const assert = require("node:assert/strict");
const { createHttpClient, parseRetryAfter, backoffDelay } = require("../lib/http");
const { HttpError, TimeoutError, NetworkError } = require("../lib/errors");
const { recordingClock, jsonResponse, textResponse, mockFetch, collectingLogger } = require("./helpers");

const URL1 = "https://api.fabric.microsoft.com/v1/workspaces";
const client = (fetchImpl, clock, extra = {}) =>
  createHttpClient({ fetchImpl, sleep: clock.sleep, now: clock.now, random: () => 1, baseDelayMs: 100, maxDelayMs: 1000, logger: collectingLogger(), ...extra });

describe("parseRetryAfter", () => {
  test("seconds", () => assert.equal(parseRetryAfter("7", 0), 7000));
  test("HTTP date", () => assert.equal(parseRetryAfter(new Date(10_000).toUTCString(), 4_000), 6000));
  test("past date is 0", () => assert.equal(parseRetryAfter(new Date(0).toUTCString(), 4_000), 0));
  test("missing or invalid", () => {
    assert.equal(parseRetryAfter(undefined, 0), null);
    assert.equal(parseRetryAfter("soon", 0), null);
  });
});

describe("backoffDelay", () => {
  test("exponential with cap", () => {
    const o = { baseDelayMs: 100, maxDelayMs: 1000, random: () => 1 };
    assert.deepEqual([0, 1, 2, 3, 4, 5].map((a) => backoffDelay(a, o)), [100, 200, 400, 800, 1000, 1000]);
  });
  test("jitter keeps at least half", () => assert.equal(backoffDelay(2, { baseDelayMs: 100, maxDelayMs: 1000, random: () => 0 }), 200));
});

describe("createHttpClient.request", () => {
  test("returns parsed JSON body, lower-case headers and status", async () => {
    const { fetchImpl } = mockFetch([["GET", /workspaces/, () => jsonResponse(200, { value: [1] }, { "X-Ms-Request-Id": "r1" })]]);
    const res = await client(fetchImpl, recordingClock()).request({ url: URL1 });
    assert.equal(res.status, 200);
    assert.deepEqual(res.body, { value: [1] });
    assert.equal(res.headers["x-ms-request-id"], "r1");
  });

  test("serializes object bodies as JSON with content-type", async () => {
    const { fetchImpl, calls } = mockFetch([["POST", /workspaces/, () => jsonResponse(201, { id: "w" })]]);
    await client(fetchImpl, recordingClock()).request({ method: "POST", url: URL1, body: { displayName: 'a"b' } });
    assert.equal(calls[0].body, '{"displayName":"a\\"b"}');
    assert.match(calls[0].headers["content-type"], /application\/json/);
  });

  test("passes Buffer bodies through untouched", async () => {
    const { fetchImpl, calls } = mockFetch([["PATCH", /x/, () => textResponse(202, "")]]);
    await client(fetchImpl, recordingClock()).request({ method: "PATCH", url: "https://h/x", body: Buffer.from("a,b\n"), headers: { "Content-Type": "application/octet-stream" } });
    assert.equal(calls[0].body, "a,b\n");
    assert.equal(calls[0].headers["content-type"], "application/octet-stream");
  });

  test("non-JSON text body is returned as text with body null", async () => {
    const { fetchImpl } = mockFetch([["GET", /x/, () => textResponse(200, "plain")]]);
    const res = await client(fetchImpl, recordingClock()).request({ url: "https://h/x" });
    assert.equal(res.text, "plain");
    assert.equal(res.body, null);
  });

  test("throws HttpError with status and body excerpt on unexpected status (no retry for 4xx)", async () => {
    const { fetchImpl, calls } = mockFetch([["POST", /x/, () => jsonResponse(400, { errorCode: "InvalidDefinition", message: "bad" })]]);
    await assert.rejects(
      client(fetchImpl, recordingClock()).request({ method: "POST", url: "https://h/x", body: {} }),
      (e) => e instanceof HttpError && e.status === 400 && /InvalidDefinition/.test(e.bodyExcerpt) && /POST/.test(e.message),
    );
    assert.equal(calls.length, 1);
  });

  test("expectStatus rejects a 2xx that is not expected", async () => {
    const { fetchImpl } = mockFetch([["POST", /x/, () => jsonResponse(200, {})]]);
    await assert.rejects(client(fetchImpl, recordingClock()).request({ method: "POST", url: "https://h/x", expectStatus: [201, 202] }), HttpError);
  });

  test("body excerpt is truncated", async () => {
    const { fetchImpl } = mockFetch([["GET", /x/, () => textResponse(404, "z".repeat(5000))]]);
    await assert.rejects(client(fetchImpl, recordingClock()).request({ url: "https://h/x" }), (e) => e.bodyExcerpt.length <= 520);
  });

  test("retries 5xx on GET with exponential backoff, then succeeds", async () => {
    const clock = recordingClock();
    const { fetchImpl, calls } = mockFetch([["GET", /x/, [() => textResponse(503, "busy"), () => textResponse(500, "err"), () => jsonResponse(200, { ok: 1 })]]]);
    const res = await client(fetchImpl, clock).request({ url: "https://h/x" });
    assert.equal(res.status, 200);
    assert.equal(calls.length, 3);
    assert.deepEqual(clock.sleeps, [100, 200]);
  });

  test("gives up after maxRetries and throws HttpError with the last status", async () => {
    const { fetchImpl, calls } = mockFetch([["GET", /x/, () => textResponse(502, "bad gateway")]]);
    await assert.rejects(client(fetchImpl, recordingClock(), { maxRetries: 2 }).request({ url: "https://h/x" }), (e) => e.status === 502);
    assert.equal(calls.length, 3);
  });

  test("429 honours Retry-After even for POST", async () => {
    const clock = recordingClock();
    const { fetchImpl, calls } = mockFetch([["POST", /x/, [() => textResponse(429, "slow", { "Retry-After": "3" }), () => jsonResponse(201, { id: 1 })]]]);
    const res = await client(fetchImpl, clock).request({ method: "POST", url: "https://h/x", body: {} });
    assert.equal(res.status, 201);
    assert.equal(calls.length, 2);
    assert.deepEqual(clock.sleeps, [3000]);
  });

  test("429 Retry-After is capped", async () => {
    const clock = recordingClock();
    const { fetchImpl } = mockFetch([["GET", /x/, [() => textResponse(429, "", { "Retry-After": "99999" }), () => jsonResponse(200, {})]]]);
    await client(fetchImpl, clock, { maxRetryAfterMs: 5000 }).request({ url: "https://h/x" });
    assert.deepEqual(clock.sleeps, [5000]);
  });

  test("does not retry 5xx on non-idempotent POST", async () => {
    const { fetchImpl, calls } = mockFetch([["POST", /x/, () => textResponse(500, "boom")]]);
    await assert.rejects(client(fetchImpl, recordingClock()).request({ method: "POST", url: "https://h/x", body: {} }), (e) => e.status === 500);
    assert.equal(calls.length, 1);
  });

  test("retries POST marked idempotent", async () => {
    const { fetchImpl, calls } = mockFetch([["POST", /x/, [() => textResponse(500, ""), () => jsonResponse(200, {})]]]);
    await client(fetchImpl, recordingClock()).request({ method: "POST", url: "https://h/x", body: {}, idempotent: true });
    assert.equal(calls.length, 2);
  });

  test("retries network errors on GET and wraps the final one in NetworkError", async () => {
    let n = 0;
    const fetchImpl = async () => { n += 1; throw new TypeError("fetch failed"); };
    await assert.rejects(client(fetchImpl, recordingClock(), { maxRetries: 2 }).request({ url: "https://h/x" }), (e) => e instanceof NetworkError && /fetch failed/.test(e.message));
    assert.equal(n, 3);
  });

  test("network error on POST is not retried", async () => {
    let n = 0;
    const fetchImpl = async () => { n += 1; throw new TypeError("fetch failed"); };
    await assert.rejects(client(fetchImpl, recordingClock()).request({ method: "POST", url: "https://h/x" }), NetworkError);
    assert.equal(n, 1);
  });

  test("per-request timeout aborts the fetch and throws TimeoutError", async () => {
    const hang = (url, init) => new Promise((_, reject) => init.signal.addEventListener("abort", () => reject(init.signal.reason)));
    await assert.rejects(
      client(hang, recordingClock(), { maxRetries: 1 }).request({ url: "https://h/x", timeoutMs: 20 }),
      (e) => e instanceof TimeoutError && /20 ms/.test(e.message),
    );
  });

  test("logs retries without the Authorization header or query string", async () => {
    const logger = collectingLogger();
    const { fetchImpl } = mockFetch([["GET", /x/, [() => textResponse(503, ""), () => jsonResponse(200, {})]]]);
    await client(fetchImpl, recordingClock(), { logger }).request({ url: "https://h/x?sig=secret", headers: { Authorization: "Bearer SECRET" } });
    const s = JSON.stringify(logger.lines);
    assert.match(s, /http_retry/);
    assert.doesNotMatch(s, /SECRET|secret/);
  });
});
