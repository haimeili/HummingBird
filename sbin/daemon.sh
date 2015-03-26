#!/bin/bash

set -x

WORKDIR="`dirname "$0"`"
WORKDIR="`cd "$WORKDIR"; pwd`"

PID_FILE="$WORKDIR/../pid/lsh.pid"
option=$1

STARTTIME=`date +%s%N | cut -b1-13`

case $option in
	(start)
	  mkdir -p $WORKDIR/../pid/
		mkdir -p $WORKDIR/../logs/
	  touch $PID_FILE
	  cd $WORKDIR/../
	  nohup java -Xmx4096m -Xms1024m -cp target/scala-2.10/LSHQuery-assembly-0.1.jar cpslab.deploy.LSHServer conf/app.conf > logs/lsh-$STARTTIME &
	  pid=$!
	  echo $pid > $PID_FILE
	;;

	(stop)
	  pid=`cat $WORKDIR/../pid/lsh.pid`
	  kill -9 $pid
	;;
esac
