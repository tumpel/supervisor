<p align="center"><img src="doc/assets/mascot.svg" alt="The tumpel salamander" width="160"></p>

# tumpel supervisor

The tumpel host supervisor. It enrols a Linux machine with KVM as a tumpel host, boots Firecracker boxes and runs coding agent turns for the control plane.

## Crates

- `tumpel-supervisor`: the host binary.
- `tumpel-guest`: the agent inside each box, shipped inside the supervisor on its guest drive.
- `tumpel-channel`: the wire protocol between the control plane and a supervisor.

The API types come from `tumpel-api`, generated in [tumpel/protocol](https://github.com/tumpel/protocol).

## Install and enrol

A host installs either a stable release or a release candidate. Both are GitHub releases. Set `version` to a stable release such as `0.3.0` or a release candidate such as `0.3.0-rc.12`, then download that release's binary and check it:

```sh
version=0.3.0
base=https://github.com/tumpel/supervisor/releases/download/v$version
curl -fL -O "$base/tumpel-supervisor-x86_64-linux" -O "$base/tumpel-supervisor-x86_64-linux.sha256"
sha256sum -c tumpel-supervisor-x86_64-linux.sha256
install -m 0755 tumpel-supervisor-x86_64-linux ./tumpel-supervisor
```

The [releases page](https://github.com/tumpel/supervisor/releases) lists every version; the release candidates are its pre-releases.

Mint an enrolment token on your own machine and pipe it to the installer on the host. Run the installer as the user that will own the boxes, never as root:

```sh
tumpel host mint-token --expires-in 600 | ssh <host> ./tumpel-supervisor install --endpoint <control plane url> --token-file -
```

An enrolled host updates itself to the supervisor its control plane stage pins, stable or release candidate, so a development host follows the release candidate its stage pins.

## Host report

Run `tumpel-supervisor` to check the host's local state once. Use `--json` for machine-readable results or `--manifest PATH` to verify the binary against a downloaded release manifest. Start supervision through the installed systemd user unit so systemd holds the running boxes’ descriptors. The unit uses the `run` alias so it can also start older releases during rollback.

## Restart and stop

Each box runs in its own systemd user scope, `tumpel-box<k>.scope`, outside the `tumpel-supervisor` unit, and systemd holds the box's connections while it runs. A restart of the unit, a rerun of `install`, a stop and start or a crash leaves the boxes running, and the next start takes them over. After a restart, each running turn continues where it was. After a crash, a turn that was running fails and the control plane offers it again, and its box stays warm for the next turn. If startup cannot attach a live box, it keeps that box and its saved files, reserves its TAP, and supervises the other boxes. A stopped unit leaves its boxes running until it starts again. To end every box, run `systemctl --user stop 'tumpel-box*.scope'`.

## Releases

Only a change to a shipped file makes a new version: a crate source, a Cargo manifest, `package.json`, the toolchain or the guest drive build. Docs, tests and CI never do. The conventional commits that changed shipped files since the latest stable tag pick the next version: a breaking change bumps major, `feat` bumps minor, and `fix` or `perf` bumps patch. Other types ride along with the next release.

- Release candidate: a push to `main` with such a change publishes the GitHub pre-release `vX.Y.Z-rc.N`, where `N` counts the commits since the latest stable tag.
- Stable release: a person runs the `promote` workflow and names one release candidate. It tags that commit `vX.Y.Z` and publishes the GitHub release as Latest. To ship several changes together, merge them and promote the last release candidate.

Every release attaches `tumpel-supervisor-x86_64-linux`, `tumpel-guest-x86_64-linux`, their `.sha256` files and `release.json`, which names the version, the protocol windows and the sha256 of the supervisor.

A failed run is rerun as it is and releases the same version. A rerun reuses the stable tag and finishes a draft release, and a published release is never overwritten.

## Development

```sh
npm install
npm run check
npm test
npm run supervisor:build
```

`npm run supervisor:build` needs `mksquashfs` (squashfs-tools). The tests behind the `host-tests` feature need KVM and run by hand.

## Licence

Apache-2.0. See `LICENSE` and `NOTICE`.
