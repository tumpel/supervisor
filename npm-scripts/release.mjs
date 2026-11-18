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
const DEV = /^v(\d+\.\d+\.\d+)-main\.(\d+)$/;
const VERSION = /^\d+\.\d+\.\d+(-main\.\d+)?$/;
const CRATES = ["tumpel-channel", "tumpel-guest", "tumpel-supervisor"];

const die = (message) => {
  throw new Error(`release: ${message}`);
};

const cargoVersion = (cargo) =>
  cargo.match(/^version = "([^"]+)"$/m)?.[1] ?? die("Cargo.toml names no workspace version");

const tumpelApiPin = (cargo) =>
  cargo.match(/^tumpel-api = "=([^"]+)"$/m)?.[1] ?? die('Cargo.toml pins no tumpel-api = "=X.Y.Z"');

const schemasPin = (packageJson) =>
  JSON.parse(packageJson).dependencies?.["@tumpel/schemas"]?.match(/tumpel-schemas-(\d+\.\d+\.\d+)\.tgz$/)?.[1] ??
  die("package.json pins no @tumpel/schemas release tarball");

export const assertProtocolPinsAgree = (schemas, api) => {
  if (schemas !== api)
    die(`@tumpel/schemas is ${schemas} and tumpel-api is ${api}; pin both to the same protocol release`);
};

const SHIPS = [
  /^Cargo\.(toml|lock)$/,
  /^rust-toolchain\.toml$/,
  /^npm-scripts\/guest-drive\.mjs$/,
  /^package\.json$/,
  /^crates\/[^/]+\/(Cargo\.toml|build\.rs|release_version\.rs|src\/.+)$/,
];

export const ships = (path) => SHIPS.some((pattern) => pattern.test(path));

const parts = (version) => version.match(SEMVER)?.slice(1, 4).map(Number) ?? null;

const semver = (tag) => parts(tag.slice(1));

const order = (a, b) => a[0] - b[0] || a[1] - b[1] || a[2] - b[2];

export const latestTag = (tags) =>
  tags
    .filter(semver)
    .sort((a, b) => order(semver(a), semver(b)))
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

export const devVersion = (latest, sinceLatest, sinceNearest) => {
  const next = nextVersion(latest, sinceLatest);
  if (!next || !sinceNearest.some(({ files }) => files.some(ships))) return "";
  return `${next}-main.${sinceLatest.length}`;
};

export const promotion = (tags, latestReleased) => {
  const tag = latestTag(tags);
  if (!latestReleased) return { version: tag.slice(1), ref: tag };
  const latest = semver(tag);
  const dev = tags
    .map((tag) => tag.match(DEV))
    .filter((match) => match && order(parts(match[1]), latest) > 0)
    .sort((a, b) => order(parts(a[1]), parts(b[1])) || a[2] - b[2])
    .at(-1);
  return dev ? { version: dev[1], ref: dev[0] } : { version: "", ref: "" };
};

const git = (...args) => execFileSync("git", args, { cwd: ROOT, encoding: "utf8" });

const commitsIn = (range) =>
  git("log", "--format=%x1e%B%x1f", "--name-only", range)
    .split("\x1e")
    .slice(1)
    .map((commit) => {
      const [message, files] = commit.split("\x1f");
      return { message: message.trim(), files: files.split("\n").filter(Boolean) };
    });

const published = async (tag) => {
  const response = await fetch(`https://api.github.com/repos/${process.env.GITHUB_REPOSITORY}/releases/tags/${tag}`, {
    headers: { authorization: `Bearer ${process.env.GH_TOKEN}` },
  });
  if (response.status === 404) return false;
  if (!response.ok) die(`GitHub answered ${response.status} for the release ${tag}`);
  return true;
};

const plan = async (build) => {
  const tags = git("tag", "--list", "v*").split("\n").filter(Boolean);
  const latest = latestTag(tags);
  if (build === "stable") return promotion(tags, await published(latest));
  const nearest = git("describe", "--tags", "--abbrev=0", "--match", "v*").trim();
  const version = devVersion(latest, commitsIn(`${latest}..HEAD`), commitsIn(`${nearest}..HEAD`));
  return { version, ref: version && git("rev-parse", "HEAD").trim() };
};

export const unpublished = (version, indexes) =>
  CRATES.filter((crate) => !indexes[crate].split("\n").some((line) => line && JSON.parse(line).vers === version));

const registryIndex = async (crate) => {
  const response = await fetch(`https://index.crates.io/${crate.slice(0, 2)}/${crate.slice(2, 4)}/${crate}`);
  if (response.status === 404) return "";
  if (!response.ok) die(`the crates.io index answered ${response.status} for ${crate}`);
  return response.text();
};

const stamp = (version) => {
  if (!VERSION.test(version)) die(`the version is ${version}; pass X.Y.Z or X.Y.Z-main.N`);
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

const pins = () =>
  assertProtocolPinsAgree(
    schemasPin(readFileSync(join(ROOT, "package.json"), "utf8")),
    tumpelApiPin(readFileSync(CARGO, "utf8")),
  );

const cli = async ([command, ...args]) => {
  if (command === "plan" && args.length === 1 && ["stable", "dev"].includes(args[0])) {
    const { version, ref } = await plan(args[0]);
    return `version=${version}\nref=${ref}`;
  }
  if (command === "pins" && args.length === 0) return pins();
  if (command === "stamp" && args.length === 1) return stamp(args[0]);
  if (command === "assets" && args.length === 2) return assets(args[0], args[1]);
  if (command === "unpublished" && args.length === 1)
    return unpublished(
      args[0],
      Object.fromEntries(await Promise.all(CRATES.map(async (c) => [c, await registryIndex(c)]))),
    ).join(" ");
  return die(
    "usage: release.mjs pins | plan <stable|dev> | stamp <X.Y.Z> | assets <X.Y.Z> <dir> | unpublished <X.Y.Z>",
  );
};

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const output = await cli(process.argv.slice(2));
  if (output !== undefined) console.log(output);
}
