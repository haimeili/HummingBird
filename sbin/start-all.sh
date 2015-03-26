#!/bin/bash

set -x

WORKDIR="`dirname "$0"`"
WORKDIR="`cd "$WORKDIR"; pwd`"

SSH_OPTS="-o StrictHostKeyChecking=no -o ConnectTimeout=5"

SLAVES=`cat $WORKDIR/../conf/slaves`

for slave in $SLAVES; do
echo $slave
ssh $SSH_OPTS $slave > /dev/null << EOF
$WORKDIR/daemon.sh start
exit
EOF
done
wait
