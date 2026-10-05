#!/bin/bash
# Run by the atmoz/sftp entrypoint (/etc/sftp.d/*) on every container start.
#
# The upload directory is a named volume. Docker creates a new volume's mount
# point as root:root, and the image's own user setup skips chown for a
# directory that already exists, so without this the delivery user cannot write
# its first file on a fresh volume.
set -eu
dir="/home/${SFTP_USER:?}/${SFTP_UPLOAD_DIR:?}"
chown "$(id -u "$SFTP_USER"):$(id -g "$SFTP_USER")" "$dir"
chmod 755 "$dir"
echo "[outbound-owner] $dir owned by $(stat -c '%U:%G' "$dir")"
