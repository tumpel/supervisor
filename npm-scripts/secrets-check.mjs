#!/usr/bin/env node
import { execFileSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

const ROOT = fileURLToPath(new URL("..", import.meta.url));
const SHAPES = [
  ["AWS access key", /\b(?:AKIA|ASIA)[0-9A-Z]{16}\b/],
  ["Anthropic API key", /\bsk-ant-[A-Za-z0-9_-]{20,}/],
  ["OpenAI API key", /\bsk-(?:proj-)?[A-Za-z0-9_-]{32,}/],
  ["Cloudflare API token", /\bcf(?:k|ut|at)_[A-Za-z0-9_-]{30,}/],
  ["Cloudflare API token", /CLOUDFLARE_API_(?:TOKEN|KEY)["']?\s*[:=]\s*["']?[A-Za-z0-9_-]{37,}/],
  ["GitHub token", /\b(?:gh[pousr]_[A-Za-z0-9]{36}|github_pat_[A-Za-z0-9_]{40,})/],
  ["Slack token", /\bxox[abprs]-[A-Za-z0-9-]{10,}/],
  ["private key", /-----BEGIN [A-Z ]*PRIVATE KEY-----/],
];

const DOCUMENTED_EXAMPLE = /EXAMPLE/;
const matches = (shape, line) => (line.match(shape) ?? []).some((value) => !DOCUMENTED_EXAMPLE.test(value));

const tracked = execFileSync("git", ["ls-files", "-z"], { cwd: ROOT, encoding: "utf8" }).split("\0").filter(Boolean);

const failures = tracked.flatMap((file) =>
  readFileSync(`${ROOT}${file}`, "utf8")
    .split("\n")
    .flatMap((line, index) =>
      SHAPES.filter(([, shape]) => matches(shape, line)).map(([name]) => `${file}:${index + 1} looks like a ${name}`),
    ),
);

if (failures.length > 0) {
  console.error(
    `secrets check failed: AGENTS.md says no shared secrets and no standing credentials.\n  ${failures.join("\n  ")}`,
  );
  console.error("Fix: remove the value, rotate it if it was real, and read it from the environment at run time.");
  process.exit(1);
}
console.log(`secrets: ${tracked.length} tracked files carry no token-shaped values`);
