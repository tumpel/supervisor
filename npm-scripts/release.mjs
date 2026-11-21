#!/usr/bin/env node
import { createHash } from "node:crypto";
import { copyFileSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { basename, join } from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = fileURLToPath(new URL("..", import.meta.url));
const CARGO = join(ROOT, "Cargo.toml");
const TARGET = "x86_64-unknown-linux-gnu";
const VERSION = /^\d+\.\d+\.\d+(-rc\.\d+)?$/;

const die = (message) => {
  throw new Error(`release: ${message}`);
};

const cargoVersion = (cargo) =>
  cargo.match(/^version = "([^"]+)"$/m)?.[1] ?? die("Cargo.toml names no workspace version");

export const tumpelApiPin = (cargo) =>
  cargo.match(/^tumpel-api = "=(\d+\.\d+\.\d+(?:-rc\.\d+)?)"$/m)?.[1] ??
  die('Cargo.toml pins no tumpel-api = "=X.Y.Z[-rc.N]" from crates.io; a git dependency is refused');

export const schemasPin = (packageJson) =>
  JSON.parse(packageJson).dependencies?.["@tumpel/schemas"]?.match(
    /tumpel-schemas-(\d+\.\d+\.\d+(?:-rc\.\d+)?)\.tgz$/,
  )?.[1] ?? die("package.json pins no @tumpel/schemas release tarball");

export const assertProtocolPinsAgree = (schemas, api) => {
  if (schemas !== api)
    die(`@tumpel/schemas is ${schemas} and tumpel-api is ${api}; pin both to the same protocol release`);
};

const stamp = (version) => {
  if (!VERSION.test(version)) die(`the version is ${version}; pass X.Y.Z or X.Y.Z-rc.N`);
  const cargo = readFileSync(CARGO, "utf8");
  const current = cargoVersion(cargo);
  const stamped = cargo
    .replace(`\nversion = "${current}"\n`, `\nversion = "${version}"\n`)
    .replaceAll(`, version = "=${current}" }`, `, version = "=${version}" }`);
  writeFileSync(CARGO, stamped);
  console.log(`stamped the workspace and its crate pins with ${version}`);
};

const sha256 = (path) => createHash("sha256").update(readFileSync(path)).digest("hex");

const assets = async (version, out) => {
  const { ChannelProtocol, GuestProtocol } = await import("@tumpel/schemas");
  const cargo = readFileSync(CARGO, "utf8");
  if (cargoVersion(cargo) !== version) die(`Cargo.toml names ${cargoVersion(cargo)}, not ${version}; stamp it first`);
  const api = tumpelApiPin(cargo);
  const schemas = JSON.parse(readFileSync(join(ROOT, "node_modules/@tumpel/schemas/package.json"), "utf8")).version;
  assertProtocolPinsAgree(schemas, api);
  mkdirSync(out, { recursive: true });
  const release = join(ROOT, "target", TARGET, "release");
  const binaries = [
    ["tumpel-supervisor", "tumpel-supervisor-x86_64-linux"],
    ["tumpel-guest", "tumpel-guest-x86_64-linux"],
  ].map(([built, name]) => {
    const asset = join(out, name);
    copyFileSync(join(release, built), asset);
    writeFileSync(`${asset}.sha256`, `${sha256(asset)}  ${name}\n`);
    return asset;
  });
  const supervisor = binaries[0];
  const manifest = {
    supervisorVersion: version,
    tumpelApiVersion: api,
    channelProtocolOldest: ChannelProtocol.Version,
    channelProtocolVersion: ChannelProtocol.Version,
    guestProtocolOldest: GuestProtocol.Oldest,
    guestProtocolVersion: GuestProtocol.Version,
    binaries: [{ architecture: "Amd64", sha256: sha256(supervisor), sizeBytes: readFileSync(supervisor).byteLength }],
  };
  writeFileSync(join(out, "release.json"), `${JSON.stringify(manifest, null, 2)}\n`);
  for (const asset of [...binaries, join(out, "release.json")]) console.log(`staged ${basename(asset)}`);
};

const pins = () =>
  assertProtocolPinsAgree(
    schemasPin(readFileSync(join(ROOT, "package.json"), "utf8")),
    tumpelApiPin(readFileSync(CARGO, "utf8")),
  );

const cli = async ([command, ...args]) => {
  if (command === "pins" && args.length === 0) return pins();
  if (command === "stamp" && args.length === 1) return stamp(args[0]);
  if (command === "assets" && args.length === 2) return assets(args[0], args[1]);
  return die("usage: release.mjs pins | stamp <X.Y.Z[-rc.N]> | assets <X.Y.Z[-rc.N]> <dir>");
};

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const output = await cli(process.argv.slice(2));
  if (output !== undefined) console.log(output);
}
