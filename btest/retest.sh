#!/bin/bash

filename=$1
mode=${2:-"profile"}
build_type=${3:-"release"}
postfix=${4:-""}

if [ -z "$mode" ]; then
    echo "Usage: $0 <run|debug|profile>"
    exit 1
fi
if [ "$mode" != "run" ] && [ "$mode" != "debug" ] && [ "$mode" != "profile" ]; then
    echo "Invalid mode: $mode. Please specify 'run', 'debug' or 'release'."
    exit 1
fi

# ����Ŀ¼�ж�
if [ "$build_type" == "debug" ]; then
    build_dir="../build_debug_lorc"
elif [ "$build_type" == "release" ]; then
    build_dir="../build_release_lorc"
else
    echo "Invalid build type: $build_type. Please specify 'debug' or 'release'."
    exit 1
fi

# Linuxʹ��LD_LIBRARY_PATH���DYLD_LIBRARY_PATH
export LD_LIBRARY_PATH=$build_dir:$LD_LIBRARY_PATH

# ��������ֲ��䣨����ʵ���������������
g++ -std=c++17 -g -O2 -fno-omit-frame-pointer -rdynamic -I../include -L$build_dir \
    -o ./bin/${filename} ./code/${filename}.cc -lrocksdb -lpthread -lz -lbz2

# run
if [ "$mode" == "run" ]; then
    ./bin/${filename}
fi

# debug
if [ "$mode" == "debug" ]; then
    gdb ./bin/${filename}
fi

# profile
if [ "$mode" == "profile" ]; then
    # ����perf����ļ���
    profile_filename=${filename}${postfix:+_$postfix}

    # ʹ��perf�������ܲ�������rootȨ�޻�����perfȨ�ޣ�
    perf record -F 99 --call-graph dwarf -g -o ./profile/${profile_filename}.data ./bin/${filename}

    # ���ɻ���ͼ������ǰ��װFlameGraph���ߣ�
    perf script -i ./profile/${profile_filename}.data | \
        stackcollapse-perf.pl | \
        flamegraph.pl > ./profile/${profile_filename}_linux_flamegraph.svg

    # �����м��ļ�
    rm -f ./profile/${profile_filename}.data
fi

du -sh db/*db*
