# ! /bin/bash

WORKDIR="`dirname "$0"`"
WORKDIR="`cd "$WORKDIR"; pwd`"

SLAVES=`cat $WORKDIR/../conf/slaves`

SSH_OPTS="-t -t -o StrictHostKeyChecking=no -o ConnectTimeout=5"

for slave in $SLAVES; do
echo $slave
ssh $slave > /dev/null << EOF
rm -rf $WORKDIR/journal $WORKDIR/snapshots
$WORKDIR/daemon.sh stop
exit
EOF
done
wait
