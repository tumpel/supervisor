#!/usr/bin/env node
import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { copyFileSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { basename, join } from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = fileURLToPath(new URL("..", import.meta.url));
const CARGO = join(ROOT, "Cargo.toml");
const TARGET = "x86_64-unknown-linux-gnu";
const SEMVER = /^(\d+)\.(\d+)\.(\d+)$/;
const CRATES = ["tumpel-channel", "tumpel-guest", "tumpel-supervisor"];

const die = (message) => {
  throw new Error(`release: ${message}`);
};

const cargoVersion = (cargo) =>
  cargo.match(/^version = "([^"]+)"$/m)?.[1] ?? die("Cargo.toml names no workspace version");

const tumpelApiPin = (cargo) =>
  cargo.match(/^tumpel-api = "=([^"]+)"$/m)?.[1] ?? die('Cargo.toml pins no tumpel-api = "=X.Y.Z"');

const SHIPS = [
  /^Cargo\.(toml|lock)$/,
  /^rust-toolchain\.toml$/,
  /^npm-scripts\/guest-drive\.mjs$/,
  /^crates\/[^/]+\/(Cargo\.toml|build\.rs|release_version\.rs|src\/.+)$/,
];

export const ships = (path) => SHIPS.some((pattern) => pattern.test(path));

const semver = (tag) => tag.slice(1).match(SEMVER)?.slice(1, 4).map(Number) ?? null;

export const latestTag = (tags) =>
  tags
    .filter(semver)
    .sort((a, b) => {
      const [x, y] = [semver(a), semver(b)];
      return x[0] - y[0] || x[1] - y[1] || x[2] - y[2];
    })
    .at(-1) ?? die("the repository carries no vX.Y.Z tag to release after");

const BREAKING = /^\w+(\([^)]*\))?!:|^BREAKING[ -]CHANGE:/m;

export const nextVersion = (latest, commits) => {
  const shipping = commits.filter(({ files }) => files.some(ships)).map(({ message }) => message);
  if (shipping.length === 0) return "";
  const [major, minor, patch] = semver(latest);
  if (shipping.some((message) => BREAKING.test(message))) return major === 0 ? `0.${minor + 1}.0` : `${major + 1}.0.0`;
  if (shipping.some((message) => /^feat(\([^)]*\))?:/.test(message))) return `${major}.${minor + 1}.0`;
  return `${major}.${minor}.${patch + 1}`;
};

const git = (...args) => execFileSync("git", args, { cwd: ROOT, encoding: "utf8" });

const commitsSince = (tag) =>
  git("log", "--format=%x1e%B%x1f", "--name-only", `${tag}..HEAD`)
    .split("\x1e")
    .slice(1)
    .map((commit) => {
      const [message, files] = commit.split("\x1f");
      return { message: message.trim(), files: files.split("\n").filter(Boolean) };
    });

export const unpublished = (version, indexes) =>
  CRATES.filter(
    (crate) => !(indexes[crate] ?? "").split("\n").some((line) => line && JSON.parse(line).vers === version),
  );

const registryIndex = async (crate) => {
  const response = await fetch(`https://index.crates.io/${crate.slice(0, 2)}/${crate.slice(2, 4)}/${crate}`);
  if (response.status === 404) return "";
  if (!response.ok) die(`the crates.io index answered ${response.status} for ${crate}`);
  return response.text();
};

const stamp = (version) => {
  if (!SEMVER.test(version)) die(`the version is ${version}; pass X.Y.Z`);
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
  const { ChannelProtocol, GuestProtocol, SupervisorReleaseSchema } = await import("@tumpel/schemas");
  const cargo = readFileSync(CARGO, "utf8");
  if (cargoVersion(cargo) !== version) die(`Cargo.toml names ${cargoVersion(cargo)}, not ${version}; stamp it first`);
  const api = tumpelApiPin(cargo);
  const schemas = JSON.parse(readFileSync(join(ROOT, "node_modules/@tumpel/schemas/package.json"), "utf8")).version;
  if (schemas !== api)
    die(`@tumpel/schemas is ${schemas} and tumpel-api is ${api}; pin both to the same protocol release`);
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
  const manifest = SupervisorReleaseSchema.parse({
    supervisorVersion: version,
    tumpelApiVersion: api,
    channelProtocolOldest: ChannelProtocol.Version,
    channelProtocolVersion: ChannelProtocol.Version,
    guestProtocolOldest: GuestProtocol.Oldest,
    guestProtocolVersion: GuestProtocol.Version,
    binaries: [{ architecture: "Amd64", sha256: sha256(supervisor), sizeBytes: readFileSync(supervisor).byteLength }],
  });
  writeFileSync(join(out, "release.json"), `${JSON.stringify(manifest, null, 2)}\n`);
  const drives = join(ROOT, "crates/tumpel-supervisor/guest-drive");
  mkdirSync(drives, { recursive: true });
  copyFileSync(join(ROOT, ".tmp/guest-drive", `${TARGET}.squashfs`), join(drives, `${TARGET}.squashfs`));
  for (const asset of [...binaries, join(out, "release.json")]) console.log(`staged ${basename(asset)}`);
};

const cli = async ([command, ...args]) => {
  if (command === "plan" && args.length === 0) {
    const latest = latestTag(git("tag", "--list", "v*").split("\n").filter(Boolean));
    return nextVersion(latest, commitsSince(latest));
  }
  if (command === "stamp" && args.length === 1) return stamp(args[0]);
  if (command === "assets" && args.length === 2) return assets(args[0], args[1]);
  if (command === "unpublished" && args.length === 1)
    return unpublished(
      args[0],
      Object.fromEntries(await Promise.all(CRATES.map(async (c) => [c, await registryIndex(c)]))),
    ).join(" ");
  return die("usage: release.mjs plan | stamp <X.Y.Z> | assets <X.Y.Z> <dir> | unpublished <X.Y.Z>");
};

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const output = await cli(process.argv.slice(2));
  if (output !== undefined) console.log(output);
}
