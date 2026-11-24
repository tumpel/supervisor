import assert from "node:assert/strict";
import { test } from "node:test";
import { dispatchDeploy, schemasPin, tumpelApiPin } from "./release.mjs";

test("the protocol pins read an exact crates.io pin and refuse a git dependency", () => {
  assert.equal(tumpelApiPin('sendfd = "0.4.5"\ntumpel-api = "=0.8.5"\n'), "0.8.5");
  assert.equal(tumpelApiPin('tumpel-api = "=0.21.0-rc.14"\n'), "0.21.0-rc.14");
  assert.throws(
    () => tumpelApiPin('tumpel-api = { git = "https://github.com/tumpel/protocol", tag = "v0.21.0-rc.14" }\n'),
    /a git dependency is refused/,
  );
  const schemas = (url) => schemasPin(JSON.stringify({ dependencies: { "@tumpel/schemas": url } }));
  assert.equal(
    schemas("https://github.com/tumpel/protocol/releases/download/v0.8.5/tumpel-schemas-0.8.5.tgz"),
    "0.8.5",
  );
  assert.equal(
    schemas("https://github.com/tumpel/protocol/releases/download/v0.21.0-rc.14/tumpel-schemas-0.21.0-rc.14.tgz"),
    "0.21.0-rc.14",
  );
});

test("an absent deploy credential notices and skips without another credential or API call", async () => {
  const notices = [];
  await dispatchDeploy(
    { token: "", readToken: "publication-only" },
    {
      notice: (line) => notices.push(line),
      request: () => assert.fail("an empty dispatch token must never call GitHub"),
    },
  );
  assert.deepEqual(notices, ["::notice::DEPLOY_DISPATCH_TOKEN is empty; skipping deploy dispatch"]);
});

test("deploy dispatch verifies the published SHA, assets and its exact main checks", async () => {
  for (const fault of ["none", "draft", "sha", "assets", "checks", "auth", "pending"]) {
    const sha = "a".repeat(40);
    const tag = "v1.2.3-rc.4";
    const calls = [];
    const sleeps = [];
    const assets = [
      "release.json",
      "tumpel-supervisor-x86_64-linux",
      "tumpel-supervisor-x86_64-linux.sha256",
      "tumpel-guest-x86_64-linux",
      "tumpel-guest-x86_64-linux.sha256",
    ].map((name) => ({
      name,
      state: "uploaded",
      size: 42,
      digest: "sha256:abc",
      browser_download_url: `https://github.com/tumpel/supervisor/releases/download/${tag}/${name}`,
    }));
    const request = async (call) => {
      calls.push(call);
      if (call.method === "POST") return;
      if (fault === "auth") throw new Error("GitHub refused access");
      if (call.path.includes("/releases/"))
        return { draft: fault === "draft", prerelease: true, tag_name: tag, assets: fault === "assets" ? [] : assets };
      if (call.path.includes("/commits/")) return { sha: fault === "sha" ? "b".repeat(40) : sha };
      assert.equal(call.path, `repos/tumpel/supervisor/actions/workflows/checks.yml/runs?head_sha=${sha}&event=push`);
      return {
        workflow_runs: [
          { head_sha: "b".repeat(40), head_branch: "main", event: "push", status: "completed", conclusion: "success" },
          {
            head_sha: sha,
            head_branch: "main",
            event: "push",
            status: fault === "pending" ? "in_progress" : "completed",
            conclusion: fault === "checks" ? "failure" : "success",
          },
        ],
      };
    };
    const result = dispatchDeploy(
      { token: "deploy-only", readToken: "source-only", repository: "tumpel/supervisor", sha, tag },
      {
        request,
        wait: async (milliseconds) => {
          sleeps.push(milliseconds);
        },
      },
    );
    if (fault === "none") {
      await result;
      assert.deepEqual(calls.at(-1), {
        token: "deploy-only",
        path: "repos/tumpel/deploy/dispatches",
        method: "POST",
        body: {
          event_type: "component-released",
          client_payload: {
            component: "supervisor",
            repository: "tumpel/supervisor",
            sha,
            tag,
            assets: assets.map((asset) => ({
              name: asset.name,
              url: asset.browser_download_url,
              size: asset.size,
              digest: asset.digest,
            })),
          },
        },
      });
    } else {
      await assert.rejects(result, /release|source SHA|assets|checks|GitHub refused/);
      assert.equal(
        calls.some((call) => call.method === "POST"),
        false,
      );
    }
    assert.equal(
      calls.filter((call) => call.method !== "POST").every((call) => call.token === "source-only"),
      true,
    );
    assert.deepEqual(sleeps, fault === "pending" ? [1000, 2000, 4000, 8000, 16000, 32000, 64000, 128000, 256000] : []);
    if (fault === "pending") assert.equal(calls.filter((call) => call.path.includes("/actions/")).length, 10);
    if (fault === "auth") assert.equal(calls.length, 1);
  }
});
