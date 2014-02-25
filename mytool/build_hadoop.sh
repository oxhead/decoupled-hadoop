#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
build_server="chsu6@bc1b3.csc.ncsu.edu"
ntp_server="time.ncsu.edu"

#ssh ${build_server} -t "sudo /etc/init.d/ntpd stop; sudo /usr/sbin/ntpdate $ntp_server; sudo /etc/init.d/ntpd start"

cmd_sync="rsync -r -v -a --exclude=*.tar.gz --exclude=*.html --exclude=*.jar --exclude=*.class ~/git/decoupled-hadoop chsu6@bc1b3.csc.ncsu.edu:/dev/shm"
cmd_remove="rm -f /dev/shm/decoupled-hadoop/hadoop-dist/target/hadoop-2.0.3-alpha.tar.gz"
cmd_build="cd /dev/shm/decoupled-hadoop;mvn package -Pdist -DskipTests -Dtar"

echo -e "\nStep 1: sync source code"
eval ${cmd_sync}
echo -e "\nStep 2: remove existing builds"
ssh $build_server -t ${cmd_remove}
echo -e "\nStep 3: build the source code"
ssh $build_server -t ${cmd_build}
