import assert from "node:assert/strict";
import { test } from "node:test";
import { latestTag, nextVersion, ships, unpublished } from "./release.mjs";

test("crate sources, manifests and what builds the binary ship; docs, tests and CI do not", () => {
  assert.equal(ships("crates/tumpel-guest/src/main.rs"), true);
  assert.equal(ships("crates/tumpel-supervisor/src/turn/mod.rs"), true);
  assert.equal(ships("crates/tumpel-supervisor/build.rs"), true);
  assert.equal(ships("crates/tumpel-channel/Cargo.toml"), true);
  assert.equal(ships("Cargo.lock"), true);
  assert.equal(ships("npm-scripts/guest-drive.mjs"), true);
  assert.equal(ships("crates/tumpel-supervisor/tests/lifecycle.rs"), false);
  assert.equal(ships("README.md"), false);
  assert.equal(ships(".github/workflows/release.yml"), false);
  assert.equal(ships("npm-scripts/release.mjs"), false);
  assert.equal(ships("package.json"), false);
});

test("commits that change nothing shipped cut no release, whatever their type", () => {
  const commits = [
    { message: "feat!: document the new protocol", files: ["README.md"] },
    { message: "ci(release): cut releases on purpose", files: [".github/workflows/release.yml"] },
    { message: "test(turn): cover a cancelled turn", files: ["crates/tumpel-supervisor/tests/turn.rs"] },
  ];
  assert.equal(nextVersion("v0.1.18", commits), "");
});

test("the commit types that changed shipped files pick the bump", () => {
  const fix = { message: "fix(pool): adopt a box", files: ["crates/tumpel-supervisor/src/pool.rs"] };
  const chore = { message: "chore(deps): bump tokio", files: ["Cargo.lock"] };
  const feat = { message: "feat(turn): end a turn", files: ["crates/tumpel-guest/src/turn.rs"] };
  const docsFeat = { message: "feat(docs): explain turns", files: ["README.md"] };
  assert.equal(nextVersion("v0.1.18", [fix]), "0.1.19");
  assert.equal(nextVersion("v0.1.18", [chore]), "0.1.19");
  assert.equal(nextVersion("v0.1.18", [fix, docsFeat]), "0.1.19");
  assert.equal(nextVersion("v0.1.18", [fix, feat]), "0.2.0");
  assert.equal(nextVersion("v1.4.2", [feat]), "1.5.0");
});

test("a breaking change bumps major, or minor while the version is 0.x", () => {
  const bang = { message: "feat(channel)!: drop the v1 frame", files: ["crates/tumpel-channel/src/lib.rs"] };
  const footer = {
    message: "fix(guest): rename the socket\n\nBREAKING CHANGE: the guest listens on a new port",
    files: ["crates/tumpel-guest/src/main.rs"],
  };
  assert.equal(nextVersion("v0.1.18", [bang]), "0.2.0");
  assert.equal(nextVersion("v0.1.18", [footer]), "0.2.0");
  assert.equal(nextVersion("v1.4.2", [bang]), "2.0.0");
  assert.equal(nextVersion("v1.4.2", [footer]), "2.0.0");
});

test("the latest release is the highest vX.Y.Z tag", () => {
  assert.equal(latestTag(["v0.1.9", "v0.1.18", "v0.0.30", "vnext"]), "v0.1.18");
  assert.throws(() => latestTag(["vnext"]), /no vX.Y.Z tag/);
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
