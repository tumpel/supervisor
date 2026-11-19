import assert from "node:assert/strict";
import { test } from "node:test";
import { schemasPin, tumpelApiPin, unpublished } from "./release.mjs";

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

test("the protocol pins read a crates.io release and a release candidate tag", () => {
  assert.equal(tumpelApiPin('sendfd = "0.4.5"\ntumpel-api = "=0.8.5"\n'), "0.8.5");
  assert.equal(
    tumpelApiPin('tumpel-api = { git = "https://github.com/tumpel/protocol", tag = "v0.21.0-rc.14" }\n'),
    "0.21.0-rc.14",
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
