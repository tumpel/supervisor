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

A push to `main` that changes the supervisor, or a manual run of the release workflow, releases the next `vX.Y.Z`. The binary, the crates and the GitHub release carry the same version.

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
