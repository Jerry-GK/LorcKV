#!/bin/bash

# arg1 = run/profile
mode=$1
task=$2

if [ -z "$mode" ]; then
    echo "Usage: $0 <run|profile>"
    exit 1
fi
if [ "$mode" != "run" ] && [ "$mode" != "debug" ] && [ "$mode" != "profile" ]; then
    echo "Invalid mode: $mode"
    exit 1
fi

# compile 
g++ -std=c++20 -g -O2 -fno-omit-frame-pointer -rdynamic ${task}.cc -o ../bin/${task}

# run
if [ "$mode" == "run" ]; then
    ../bin/${task}
fi

# debug
if [ "$mode" == "debug" ]; then
    gdb ../bin/${task}
fi

# profile
if [ "$mode" == "profile" ]; then
    echo "Profiling..."
    perf record -F 99 --call-graph dwarf -g -o ../profile/profile_${task}.data ../bin/${task}

    # 生成火焰图（需提前安装FlameGraph工具）
    perf script -i ../profile/profile_${task}.data | \
    stackcollapse-perf.pl | \
    flamegraph.pl > ../profile/${task}.svg
    rm p../rofile/profile_${task}.data
fi

# clean
# rm ../bin/${task}