import assert from "node:assert/strict";
import { test } from "node:test";
import { schemasPin, tumpelApiPin } from "./release.mjs";

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
