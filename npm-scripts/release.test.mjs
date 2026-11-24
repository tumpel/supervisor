import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { test } from "node:test";
import { assertPublishedRelease, assertSourceSha, schemasPin, tumpelApiPin } from "./release.mjs";

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

test("a release tag must name the checked source commit", () => {
  assertSourceSha({ sha: "a".repeat(40) }, "a".repeat(40));
});

test("a tag pointing at another commit is refused", () => {
  assert.throws(() => assertSourceSha({ sha: "b".repeat(40) }, "a".repeat(40)), /does not name this source SHA/);
});

test("a draft candidate is refused", () => {
  assert.throws(
    () => assertPublishedRelease({ draft: true, prerelease: true, tag_name: "v1.2.3-rc.4" }, "v1.2.3-rc.4"),
    /published release candidate/,
  );
});

test("a stable release is refused", () => {
  assert.throws(
    () => assertPublishedRelease({ draft: false, prerelease: false, tag_name: "v1.2.3" }, "v1.2.3-rc.4"),
    /published release candidate/,
  );
});

test("a release with another tag is refused", () => {
  assert.throws(
    () => assertPublishedRelease({ draft: false, prerelease: true, tag_name: "v1.2.3-rc.3" }, "v1.2.3-rc.4"),
    /published release candidate/,
  );
});

test("a candidate missing its uploaded assets is refused", () => {
  assert.throws(
    () =>
      assertPublishedRelease({ draft: false, prerelease: true, tag_name: "v1.2.3-rc.4", assets: [] }, "v1.2.3-rc.4"),
    /missing its uploaded assets/,
  );
});

test("a published candidate has all five uploaded assets", () => {
  assert.doesNotThrow(() =>
    assertPublishedRelease(
      {
        draft: false,
        prerelease: true,
        tag_name: "v1.2.3-rc.4",
        assets: [
          { name: "release.json", state: "uploaded", size: 1 },
          { name: "tumpel-supervisor-x86_64-linux", state: "uploaded", size: 2 },
          { name: "tumpel-supervisor-x86_64-linux.sha256", state: "uploaded", size: 3 },
          { name: "tumpel-guest-x86_64-linux", state: "uploaded", size: 4 },
          { name: "tumpel-guest-x86_64-linux.sha256", state: "uploaded", size: 5 },
        ],
      },
      "v1.2.3-rc.4",
    ),
  );
});

test("an empty deploy token notices and skips in the real CLI", () => {
  const result = spawnSync(process.execPath, [new URL("release.mjs", import.meta.url).pathname, "dispatch-deploy"], {
    env: { DEPLOY_DISPATCH_TOKEN: "" },
    encoding: "utf8",
  });
  assert.equal(result.status, 0, result.stderr);
  assert.equal(result.stdout, "::notice::DEPLOY_DISPATCH_TOKEN is empty; skipping deploy dispatch\n");
});
