#!/usr/bin/env node
import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { chmodSync, existsSync, mkdirSync, readFileSync, renameSync, rmSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { BaseLayout } from "@tumpel/schemas";

const ROOT = dirname(dirname(fileURLToPath(import.meta.url)));
const TARGET = process.env.TUMPEL_TARGET ?? "x86_64-unknown-linux-gnu";
const GUEST_BIN = process.env.TUMPEL_GUEST_BIN ?? join(ROOT, "target", TARGET, "release", "tumpel-guest");
const OUT = join(ROOT, ".tmp", "guest-drive");
const DRIVE = join(OUT, `${TARGET}.squashfs`);
const PACK_SECONDS = 120;

const INIT = `#!/bin/sh
set -eu
PATH=/usr/sbin:/usr/bin:/sbin:/bin
export PATH
GUEST=/opt/tumpel/guest
fail() {
  echo "tumpel-init: $1 failed: $2" >&2
  exit 1
}
run() {
  step=$1
  shift
  output=$("$@" 2>&1) || fail "$step" "$output"
}
argument() {
  for word in $(cat /proc/cmdline); do
    case $word in "$1="*) echo "\${word#*=}" ;; esac
  done
}
mountpoint -q /proc || run proc mount -t proc proc /proc
mountpoint -q /dev || run dev mount -t devtmpfs devtmpfs /dev
expected=$(cat "$GUEST/base-layout")
[ -f /etc/tumpel/base-layout ] || fail base-layout "the base image has no /etc/tumpel/base-layout and this guest drive needs base layout $expected; push an image release tag images-vX.Y.Z on the supervisor's commit"
actual=$(cat /etc/tumpel/base-layout)
[ "$actual" = "$expected" ] || fail base-layout "the base image has base layout $actual and this guest drive needs $expected; push an image release tag images-vX.Y.Z on the supervisor's commit"
harness=$(argument tumpel.harness)
[ -n "$harness" ] || fail harness "the kernel command line names no tumpel.harness=<harness>/<version>"
prebuild=$(argument tumpel.prebuild)
restore=$(argument tumpel.restore)
run scratch mount -t tmpfs -o mode=0755 tmpfs /run
if ! mount -t ext4 -o noatime /dev/vdb /overlay 2>/dev/null; then
  run overlay mkfs.ext4 -q -F -I 256 /dev/vdb
  run overlay mount -t ext4 -o noatime /dev/vdb /overlay
  mkdir -p /overlay/upper
  if [ -n "$restore" ]; then
    run restore tar -x -p --numeric-owner --xattrs --xattrs-include='trusted.overlay.*' -f "/dev/$restore" -C /overlay/upper
  fi
fi
mkdir -p /overlay/upper /overlay/work
lower=/
if [ -n "$prebuild" ]; then
  mkdir -p /run/tumpel/prebuild
  run prebuild mount -t squashfs -o ro "/dev/$prebuild" /run/tumpel/prebuild
  lower=/run/tumpel/prebuild:/
fi
run overlay mount -t overlay overlay -o "lowerdir=$lower,upperdir=/overlay/upper,workdir=/overlay/work" /mnt
[ -d /mnt/opt/harness ] || fail harness "the base image has no /opt/harness"
run harness mount -t tmpfs -o mode=0755 tmpfs /mnt/opt/harness
mkdir -p "/mnt/opt/harness/$harness"
run harness mount -t squashfs -o ro /dev/vdd "/mnt/opt/harness/$harness"
run guest mount --move "$GUEST" /mnt/opt/tumpel/guest
run units mount -t tmpfs -o mode=0755,nosuid,nodev tmpfs /mnt/run
mkdir -p /mnt/run/systemd/system/multi-user.target.wants
ln -s /opt/tumpel/guest/tumpel-guest.service /mnt/run/systemd/system/tumpel-guest.service
ln -s /opt/tumpel/guest/tumpel-guest.service /mnt/run/systemd/system/multi-user.target.wants/tumpel-guest.service
run dev mount -t devtmpfs devtmpfs /mnt/dev
umount /proc
cd /mnt
run pivot pivot_root . rom
exec chroot . /sbin/init
`;

const UNIT = `[Unit]
Description=tumpel guest agent
DefaultDependencies=no
After=local-fs.target

[Service]
ExecStart=/opt/tumpel/guest/tumpel-guest
Restart=always
RestartSec=1
Delegate=yes
StandardOutput=journal+console
StandardError=journal+console

[Install]
WantedBy=multi-user.target
`;

const die = (message) => {
  console.error(`guest:drive: ${message}`);
  process.exit(1);
};

const mksquashfs = () => {
  const found = [...(process.env.PATH ?? "").split(":"), "/usr/sbin", "/sbin"]
    .map((dir) => join(dir, "mksquashfs"))
    .find((path) => existsSync(path));
  if (!found) die("mksquashfs is not installed; install squashfs-tools on the build machine");
  return found;
};

const main = () => {
  if (!existsSync(GUEST_BIN)) die(`${GUEST_BIN} is missing; build it with TUMPEL_TARGET=${TARGET} npm run guest:build`);
  const tool = mksquashfs();
  const tree = join(OUT, `.tree-${process.pid}`);
  mkdirSync(tree, { recursive: true });
  try {
    chmodSync(tree, 0o755);
    writeFileSync(join(tree, "tumpel-guest"), readFileSync(GUEST_BIN), { mode: 0o755 });
    writeFileSync(join(tree, "init"), INIT, { mode: 0o755 });
    writeFileSync(join(tree, "tumpel-guest.service"), UNIT, { mode: 0o644 });
    writeFileSync(join(tree, "base-layout"), `${BaseLayout.Version}\n`, { mode: 0o644 });
    const staged = `${DRIVE}.part`;
    rmSync(staged, { force: true });
    const result = spawnSync(
      tool,
      [
        tree,
        staged,
        "-noappend",
        "-quiet",
        "-no-progress",
        "-comp",
        "zstd",
        "-all-root",
        "-all-time",
        "0",
        "-mkfs-time",
        "0",
      ],
      { stdio: ["ignore", "inherit", "inherit"], timeout: PACK_SECONDS * 1000 },
    );
    if (result.error?.code === "ETIMEDOUT")
      die(`mksquashfs took longer than ${PACK_SECONDS}s packing ${tree}; check the build machine's disk`);
    if (result.status !== 0) die(`mksquashfs exited with ${result.status ?? result.signal} packing ${tree}`);
    renameSync(staged, DRIVE);
  } finally {
    rmSync(tree, { recursive: true, force: true });
  }
  const digest = createHash("sha256").update(readFileSync(DRIVE)).digest("hex");
  console.log(`${DRIVE} ${digest}`);
};

main();
