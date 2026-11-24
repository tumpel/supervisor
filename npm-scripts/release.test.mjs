import assert from "node:assert/strict";
import { execFileSync, spawnSync } from "node:child_process";
import { copyFileSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { fileURLToPath } from "node:url";
import { data, Evaluator, Lexer, Parser } from "@actions/expressions";
import { parse } from "yaml";
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

const workflow = parse(readFileSync(new URL("../.github/workflows/release.yml", import.meta.url), "utf8"));

const evaluate = (expression, context, extensions = []) => {
  const source = expression.trim().replace(/^\$\{\{\s*|\s*\}\}$/g, "");
  const { tokens } = new Lexer(source).lex();
  const ast = new Parser(tokens, Object.keys(context), extensions).parse();
  return new Evaluator(
    ast,
    JSON.parse(JSON.stringify(context), data.reviver),
    new Map(extensions.map((fn) => [fn.name, fn])),
  )
    .evaluate()
    .coerceString();
};

test("only successful same-repository main pushes share the release queue", () => {
  const ahead = "b".repeat(40);
  const checked = "a".repeat(40);
  const accepted = {
    conclusion: "success",
    event: "push",
    head_branch: "main",
    head_repository: { full_name: "tumpel/supervisor" },
    head_sha: checked,
  };
  const { concurrency, jobs } = workflow;
  assert.equal(concurrency["cancel-in-progress"], false);
  for (const [run_id, changes, group] of [
    [100, {}, "release"],
    [101, { conclusion: "failure", head_sha: ahead }, "release-skipped-101"],
    [102, { conclusion: "cancelled", head_sha: ahead }, "release-skipped-102"],
    [103, { event: "pull_request" }, "release-skipped-103"],
    [104, { head_branch: "topic" }, "release-skipped-104"],
    [105, { head_repository: { full_name: "fork/supervisor" } }, "release-skipped-105"],
  ]) {
    const context = {
      github: {
        repository: "tumpel/supervisor",
        run_id,
        sha: ahead,
        event: { workflow_run: { ...accepted, ...changes } },
      },
    };
    assert.equal(evaluate(concurrency.group, context), group);
    assert.equal(evaluate(jobs.plan.if, context), String(group === "release"));
    if (group === "release") {
      const env = { SOURCE_SHA: evaluate(workflow.env.SOURCE_SHA, context) };
      assert.equal(env.SOURCE_SHA, checked);
      for (const job of [jobs.plan, jobs.candidate]) {
        const checkout = job.steps.find((step) => step.uses?.startsWith("actions/checkout@"));
        assert.equal(evaluate(checkout.with.ref, { env }), checked);
      }
    }
  }
});

const candidateAssets = (tag) =>
  [
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

const dispatchBody = (sha, tag, assets) => ({
  event_type: "component-released",
  client_payload: {
    component: "supervisor",
    repository: "tumpel/supervisor",
    sha,
    tag,
    assets: assets.map(({ name, browser_download_url: url, size, digest }) => ({ name, url, size, digest })),
  },
});

test("dispatch reads release evidence before sending with the deploy credential", () => {
  const sha = "a".repeat(40);
  const tag = "v1.2.3-rc.4";
  const assets = candidateAssets(tag);
  const repo = "repos/tumpel/supervisor";
  const releasePath = `${repo}/releases/tags/${tag}`;
  const commitPath = `${repo}/commits/${tag}`;
  const deployPath = "repos/tumpel/deploy/dispatches";
  for (const [fault, error, paths] of [
    ["valid", null, [releasePath, commitPath, deployPath]],
    ["draft", /published release candidate/, [releasePath]],
    ["stable", /published release candidate/, [releasePath]],
    ["assets", /missing its uploaded assets/, [releasePath]],
    ["sha", /does not name this source SHA/, [releasePath, commitPath]],
    ["auth", /GitHub refused access/, [releasePath]],
  ]) {
    const calls = [];
    const dispatch = () =>
      dispatchDeploy(
        { token: "deploy-only", readToken: "source-only", repository: "tumpel/supervisor", sha, tag },
        (call) => {
          calls.push(call);
          switch (call.path) {
            case releasePath:
              if (fault === "auth") throw new Error("GitHub refused access");
              return {
                draft: fault === "draft",
                prerelease: fault !== "stable",
                tag_name: tag,
                assets: fault === "assets" ? assets.slice(1) : assets,
              };
            case commitPath:
              return { sha: fault === "sha" ? "b".repeat(40) : sha };
            case deployPath:
              return;
            default:
              assert.fail(call.path);
          }
        },
      );

    if (error) assert.throws(dispatch, error, fault);
    else dispatch();
    assert.deepEqual(
      calls,
      paths.map((path) =>
        path === deployPath
          ? { token: "deploy-only", method: "POST", path, body: dispatchBody(sha, tag, assets) }
          : { token: "source-only", path },
      ),
      fault,
    );
  }
});

test("the workflow publishes the checked commit before deploy dispatch", (t) => {
  const dir = mkdtempSync(join(tmpdir(), "supervisor-release-"));
  t.after(() => rmSync(dir, { recursive: true, force: true }));
  const git = (...args) =>
    execFileSync("git", ["-c", "user.name=Release Test", "-c", "user.email=release@example.test", ...args], {
      cwd: dir,
      encoding: "utf8",
      stdio: ["ignore", "pipe", "pipe"],
    }).trim();
  git("init", "-b", "main");
  git("init", "--bare", "origin.git");
  git("remote", "add", "origin", "origin.git");
  git("commit", "--allow-empty", "-m", "checked commit");
  const checked = git("rev-parse", "HEAD");
  git("commit", "--allow-empty", "-m", "later branch tip");
  const ahead = git("rev-parse", "HEAD");
  git("push", "origin", "main");

  const steps = workflow.jobs.candidate.steps;
  const publish = steps.find(({ id }) => id === "publish");
  const deploy = steps.find(({ id }) => id === "dispatch-deploy");
  assert.ok(steps.indexOf(publish) < steps.indexOf(deploy));
  assert.equal(publish["continue-on-error"], undefined);
  assert.equal(deploy["continue-on-error"], true);
  const output = join(dir, "output");
  const log = join(dir, "github.jsonl");
  const release = join(dir, "release.json");
  const releaseScript = fileURLToPath(new URL("release.mjs", import.meta.url));
  mkdirSync(join(dir, "npm-scripts"));
  copyFileSync(releaseScript, join(dir, "npm-scripts/release.mjs"));
  mkdirSync(join(dir, ".tmp/release"), { recursive: true });
  writeFileSync(join(dir, ".tmp/release/release.json"), "{}");
  writeFileSync(
    join(dir, "gh"),
    `#!${process.execPath}
const { appendFileSync, existsSync, readFileSync, writeFileSync } = require("node:fs");
const { execFileSync } = require("node:child_process");
const args = process.argv.slice(2);
const { GH_TOKEN, TEST_LOG, TEST_RELEASE, TEST_ASSETS, TEST_PUBLICATION, TAG } = process.env;
const body = args.includes("--input") ? JSON.parse(readFileSync(0, "utf8")) : null;
appendFileSync(TEST_LOG, JSON.stringify({ args, token: GH_TOKEN, body }) + "\\n");
const read = () => JSON.parse(readFileSync(TEST_RELEASE, "utf8"));
if (args[0] === "release") {
  if (TEST_PUBLICATION === "failed") process.exit(1);
  if (args[1] === "upload") process.exit(0);
  if (args[1] !== "create" && args[1] !== "edit") process.exit(1);
  if (args[1] === "create" && !args.includes("--verify-tag")) process.exit(1);
  execFileSync("git", ["ls-remote", "--exit-code", "origin", "refs/tags/" + TAG]);
  writeFileSync(TEST_RELEASE, JSON.stringify({
    draft: false, prerelease: true, tag_name: TAG, assets: JSON.parse(TEST_ASSETS),
  }));
} else if (args[1] === "--paginate") {
  if (existsSync(TEST_RELEASE)) console.log(read().draft);
} else if (args[3] === "repos/tumpel/supervisor/releases/tags/" + TAG) {
  console.log(JSON.stringify(read()));
} else if (args[3] === "repos/tumpel/supervisor/commits/" + TAG) {
  const found = execFileSync("git", ["ls-remote", "--exit-code", "origin", "refs/tags/" + TAG], { encoding: "utf8" });
  console.log(JSON.stringify({ sha: found.split(/\\s/)[0] }));
} else if (args[2] !== "POST" || args[3] !== "repos/tumpel/deploy/dispatches") {
  process.exit(1);
}
`,
    { mode: 0o700 },
  );

  for (const [number, publication, token, expected] of [
    [1, "new", "deploy-only", ["list", "create", "release", "commit", "dispatch"]],
    [2, "new", "", ["list", "create"]],
    [3, "failed", "deploy-only", ["list", "create"]],
    [4, "collision", "deploy-only", []],
    [5, "draft", "deploy-only", ["list", "upload", "edit", "release", "commit", "dispatch"]],
    [6, "published", "deploy-only", ["list"]],
    [7, "annotated", "", ["list", "create"]],
  ]) {
    const tag = `v1.2.3-rc.${number}`;
    const ref = `refs/tags/${tag}`;
    if (publication === "collision") git("push", "origin", `${ahead}:${ref}`);
    if (publication === "annotated") {
      git("tag", "-a", tag, checked, "-m", "checked source");
      git("push", "origin", ref);
    }
    const assets = candidateAssets(tag);
    rmSync(release, { force: true });
    if (["draft", "published"].includes(publication)) {
      git("push", "origin", `${checked}:${ref}`);
      writeFileSync(
        release,
        JSON.stringify({ draft: publication === "draft", prerelease: true, tag_name: tag, assets }),
      );
    }
    writeFileSync(output, "");
    writeFileSync(log, "");
    const env = {
      PATH: `${dir}:${process.env.PATH}`,
      SOURCE_SHA: checked,
      TAG: tag,
      GH_TOKEN: "source-only",
      GITHUB_REPOSITORY: "tumpel/supervisor",
      GITHUB_OUTPUT: output,
      TEST_LOG: log,
      TEST_RELEASE: release,
      TEST_ASSETS: JSON.stringify(assets),
      TEST_PUBLICATION: publication,
    };
    const run = (script, env) => spawnSync("bash", ["-e", "-c", script], { cwd: dir, encoding: "utf8", env });
    const result = run(publish.run, env);
    const succeeded = !["failed", "collision"].includes(publication);
    assert.equal(result.status, succeeded ? 0 : 1, result.stderr);
    const published = new URLSearchParams(readFileSync(output, "utf8").trim()).get("published") ?? "";
    const eligible = evaluate(deploy.if, { steps: { publish: { outputs: { published } } } }, [
      { name: "success", minArgs: 0, maxArgs: 0, call: () => new data.BooleanData(result.status === 0) },
    ]);
    assert.equal(eligible, String(succeeded && publication !== "published"), publication);
    if (eligible === "true") {
      const dispatched = run(deploy.run, { ...env, DEPLOY_DISPATCH_TOKEN: token });
      assert.equal(dispatched.status, 0, dispatched.stderr);
      assert.equal(
        dispatched.stdout,
        token ? "" : "::notice::DEPLOY_DISPATCH_TOKEN is empty; skipping deploy dispatch\n",
      );
    }
    assert.equal(
      git("--git-dir=origin.git", "rev-parse", `${ref}^{commit}`),
      publication === "collision" ? ahead : checked,
    );
    const calls = readFileSync(log, "utf8")
      .trim()
      .split("\n")
      .filter(Boolean)
      .map((line) => JSON.parse(line));
    assert.deepEqual(
      calls.map(({ args }) => {
        if (args[0] === "release") return args[1];
        if (args[1] === "--paginate") return "list";
        if (args[2] === "POST") return "dispatch";
        return args[3].includes("/commits/") ? "commit" : "release";
      }),
      expected,
      publication,
    );
    for (const call of calls) {
      assert.equal(call.token, call.args[2] === "POST" ? "deploy-only" : "source-only");
      if (call.args[2] === "POST") assert.deepEqual(call.body, dispatchBody(checked, tag, assets));
    }
  }
});

test("an empty deploy token notices and skips in the real CLI", () => {
  const result = spawnSync(process.execPath, [new URL("release.mjs", import.meta.url).pathname, "dispatch-deploy"], {
    env: { DEPLOY_DISPATCH_TOKEN: "" },
    encoding: "utf8",
  });
  assert.equal(result.status, 0, result.stderr);
  assert.equal(result.stdout, "::notice::DEPLOY_DISPATCH_TOKEN is empty; skipping deploy dispatch\n");
});
