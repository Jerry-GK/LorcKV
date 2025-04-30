#!/bin/bash

# arg1 = run/profile
# binName="testVecLORC"
binName="testSliceVecLORC"

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
make clean && make -j$(nproc)
make cleanObjects

# run
if [ "$mode" == "run" ]; then
    ./bin/$binName
fi

# debug
if [ "$mode" == "debug" ]; then
    gdb ./bin/$binName
fi

# profile
if [ "$mode" == "profile" ]; then
    echo "Profiling..."
    perf record -F 99 --call-graph dwarf -g -o ./profile/profile.data ./bin/$binName

    # 生成火焰图（需提前安装FlameGraph工具）
    perf script -i ./profile/profile.data | \
    stackcollapse-perf.pl | \
    flamegraph.pl > ./profile/lorc_flamegraph.svg
    rm profile/profile.data
fi

# clean
make cleanTarget