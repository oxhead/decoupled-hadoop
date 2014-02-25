#!/bin/bash

user="chsu6"
home_dir="/home/$user"
build_server="bc1b3.csc.ncsu.edu"
hadoop_file_remote="/dev/shm/decoupled-hadoop/hadoop-dist/target/hadoop-2.0.3-alpha.tar.gz"
hadoop_file_local="$home_dir/hadoop-2.0.3-alpha.tar.gz"
hadoop_runtime_dir=$home_dir/hadoop_runtime

# 1) Download the latested build
rm $hadoop_file_local
scp $user@$build_server:$hadoop_file_remote $hadoop_file_local

# 2) Prepare directories
unlink $home_dir/hadoop
rm -rf $home_dir/hadoop-2.0.3-alpha
cd $home_dir; tar zxvf $hadoop_file_local && ln -s $home_dir/hadoop-2.0.3-alpha $home_dir/hadoop && mkdir $home_dir/hadoop/conf

# 3) Prepare
rm -rf $hadoop_runtime_dir/yarn/*
