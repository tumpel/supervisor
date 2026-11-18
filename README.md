# tumpel supervisor

The tumpel host supervisor. It enrols a Linux machine with KVM as a tumpel host, boots Firecracker boxes and runs coding agent turns for the control plane.

## Crates

- `tumpel-supervisor`: the host binary.
- `tumpel-guest`: the agent inside each box, shipped inside the supervisor on its guest drive.
- `tumpel-channel`: the wire protocol between the control plane and a supervisor.

The API types come from `tumpel-api`, generated in [tumpel/protocol](https://github.com/tumpel/protocol).

## Install and enrol

Download `tumpel-supervisor-x86_64-linux` and its `.sha256` from the [latest release](https://github.com/tumpel/supervisor/releases/latest), then check and install it:

```sh
sha256sum -c tumpel-supervisor-x86_64-linux.sha256
install -m 0755 tumpel-supervisor-x86_64-linux ./tumpel-supervisor
```

Mint an enrolment token on your own machine and pipe it to the installer on the host. Run the installer as the user that will own the boxes, never as root:

```sh
tumpel host mint-token --expires-in 600 | ssh <host> ./tumpel-supervisor install --endpoint <control plane url> --token-file -
```

`cargo install tumpel-supervisor` builds the same binary, with the guest drive that ships inside the crate.

## Restart and stop

Each box runs in its own systemd user scope, `tumpel-box<k>.scope`, outside the `tumpel-supervisor` unit, and systemd holds the box's connections while it runs. A restart of the unit, a rerun of `install`, a stop and start or a crash leaves the boxes running, and the next start takes them over. After a restart, each running turn continues where it was. After a crash, a turn that was running fails and the control plane offers it again, and its box stays warm for the next turn. The next start stops every box it cannot take over. A stopped unit leaves its boxes running until it starts again. To end every box, run `systemctl --user stop 'tumpel-box*.scope'`.

## Releases

The supervisor ships two builds, and neither needs a person to cut it.

- Dev build: every push to `main` that changes a shipped file publishes a GitHub pre-release `vX.Y.Z-main.N`. A shipped file is a crate source, a Cargo manifest, the toolchain or the guest drive build. `X.Y.Z` is the next stable version and `N` counts the commits since the latest stable release, so semver orders every dev build after the latest stable release and before the next one. A dev build never touches crates.io.
- Stable build: once a day at 04:00 UTC, the newest dev build without a stable release is promoted. The run builds that dev build's commit as `X.Y.Z`, publishes the crates to crates.io, and publishes the GitHub release as Latest. A day with no new dev build releases nothing.

The conventional commit types since the latest stable release pick `X.Y.Z`: a breaking change bumps major, or minor while the version is 0.x, a `feat` bumps minor, and any other type bumps patch. Docs, tests and CI changes never build.

Every release, dev or stable, attaches the same assets under the same names: `tumpel-supervisor-x86_64-linux`, `tumpel-guest-x86_64-linux`, their `.sha256` files and `release.json`. Fetch one by tag from `https://github.com/tumpel/supervisor/releases/download/v<version>/<asset>`. `release.json` names the version, the protocol windows and the sha256 of the supervisor.

The tag appears only when the GitHub release publishes. A rerun of a failed release keeps its version, skips crates already on crates.io and finishes the GitHub release, and the next day's promotion retries a failed stable release on its own.

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
