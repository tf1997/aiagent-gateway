#!/bin/sh
set -eu
# systemd 252 creates credentials in a temporary mount namespace, then moves
# them into /run. Propagation must reach the service namespace. /run is a
# container-only tmpfs; this container never bind-mounts host filesystems.
mount --make-rshared /run
exec /lib/systemd/systemd
