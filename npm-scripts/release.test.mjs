import assert from "node:assert/strict";
import { test } from "node:test";
import { unpublished } from "./release.mjs";

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
