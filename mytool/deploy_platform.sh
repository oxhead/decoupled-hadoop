#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
user="chsu6"
home_dir=/home/$user
deploy_script="deploy_node.sh"

function deploy() {
	node=$1
	echo "[$node] Copy script"
        scp $DIR/$deploy_script $user@$node:$home_dir
        echo "[$node] Execute the script"
        ssh $user@$node bash $deploy_script
}

for (( i=1; i<=6; i++ )); do
	node="power$i.csc.ncsu.edu"
	deploy $node
done

for (( i=2; i<=14; i++ )); do
        node="bc1b$i.csc.ncsu.edu"
        deploy $node
done


