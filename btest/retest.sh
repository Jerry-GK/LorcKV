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

# 构建目录判断
if [ "$build_type" == "debug" ]; then
    build_dir="../build_debug_lorc"
elif [ "$build_type" == "release" ]; then
    build_dir="../build_release_lorc"
else
    echo "Invalid build type: $build_type. Please specify 'debug' or 'release'."
    exit 1
fi

# Linux使用LD_LIBRARY_PATH替代DYLD_LIBRARY_PATH
export LD_LIBRARY_PATH=$build_dir:$LD_LIBRARY_PATH

# 编译命令保持不变（根据实际需求调整参数）
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
    # 设置perf输出文件名
    profile_filename=${filename}${postfix:+_$postfix}

    # 使用perf进行性能采样（需root权限或配置perf权限）
    perf record -F 99 --call-graph dwarf -g -o ./profile/${profile_filename}.data ./bin/${filename}

    # 生成火焰图（需提前安装FlameGraph工具）
    perf script -i ./profile/${profile_filename}.data | \
        stackcollapse-perf.pl | \
        flamegraph.pl > ./profile/${profile_filename}_linux_flamegraph.svg

    # 清理中间文件
    rm -f ./profile/${profile_filename}.data
fi

du -sh db/*db*
