#!/bin/bash

# arg1 = run/profile
mode=$1
if [ -z "$mode" ]; then
    echo "Usage: $0 <run|profile>"
    exit 1
fi
if [ "$mode" != "run" ] && [ "$mode" != "debug" ] && [ "$mode" != "profile" ]; then
    echo "Invalid mode: $mode"
    exit 1
fi

# compile 
g++ -std=c++17 -g -O2 -fno-omit-frame-pointer -rdynamic test_string.cc -o ./bin/testString

# run
if [ "$mode" == "run" ]; then
    ./bin/testLORC
fi

# debug
if [ "$mode" == "debug" ]; then
    gdb ./bin/testString
fi

# profile
if [ "$mode" == "profile" ]; then
    echo "Profiling..."
    perf record -F 99 --call-graph dwarf -g -o ./profile/profile_Str.data ./bin/testString

    # 生成火焰图（需提前安装FlameGraph工具）
    perf script -i ./profile/profile_Str.data | \
    stackcollapse-perf.pl | \
    flamegraph.pl > ./profile/test_string.svg
    rm profile/profile_Str.data
fi

# clean
rm bin/testString