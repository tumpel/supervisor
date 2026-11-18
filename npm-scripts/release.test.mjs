import assert from "node:assert/strict";
import { test } from "node:test";
import { releaseVersion, unpublished } from "./release.mjs";

test("a rerun on a tagged commit releases its tag again instead of bumping", () => {
  assert.equal(releaseVersion(["v0.1.18", "v0.1.19"], ["v0.1.19"], "minor", "0.1.0"), "0.1.19");
});

test("an untagged commit bumps the latest release tag", () => {
  const tags = ["v0.1.9", "v0.1.18", "v0.0.30", "vnext"];
  assert.equal(releaseVersion(tags, [], "patch", "0.1.0"), "0.1.19");
  assert.equal(releaseVersion(tags, [], "minor", "0.1.0"), "0.2.0");
  assert.equal(releaseVersion(tags, [], "major", "0.1.0"), "1.0.0");
});

test("the first release takes the workspace version", () => {
  assert.equal(releaseVersion([], [], "patch", "0.1.0"), "0.1.0");
});

test("a commit with two release tags refuses to pick one", () => {
  assert.throws(
    () => releaseVersion(["v0.1.19", "v0.1.20"], ["v0.1.19", "v0.1.20"], "patch", "0.1.0"),
    /v0.1.19 and v0.1.20/,
  );
});

test("crates already on crates.io at the version are skipped, in publish order", () => {
  const indexes = {
    "tumpel-channel": '{"name":"tumpel-channel","vers":"0.1.18"}\n{"name":"tumpel-channel","vers":"0.1.19"}\n',
    "tumpel-guest": '{"name":"tumpel-guest","vers":"0.1.19"}\n',
    "tumpel-supervisor": '{"name":"tumpel-supervisor","vers":"0.1.18"}\n',
  };
  assert.deepEqual(unpublished("0.1.19", indexes), ["tumpel-supervisor"]);
  assert.deepEqual(unpublished("0.1.20", indexes), ["tumpel-channel", "tumpel-guest", "tumpel-supervisor"]);
  assert.deepEqual(unpublished("0.1.18", { ...indexes, "tumpel-guest": "" }), ["tumpel-guest"]);
});
