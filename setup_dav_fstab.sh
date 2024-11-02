#!/usr/bin/env sh
set -e
USER=`whoami`
URL="http://127.0.0.1:4918"
MOUNT_POINT="/mnt/dogbox_localhost"
FSTAB_LINE="$URL $MOUNT_POINT davfs rw,user,uid=$USER,noauto,exec 0 0"
FSTAB_ECHO="echo \"$FSTAB_LINE\" >> /etc/fstab"

sudo apt install davfs2 || exit 1
sudo mkdir "$MOUNT_POINT" || exit 1
sudo sh -c "$FSTAB_ECHO" || exit 1
