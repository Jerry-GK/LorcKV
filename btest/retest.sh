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

# Build directory determination
if [ "$build_type" == "debug" ]; then
    build_dir="../build_debug_lorc"
elif [ "$build_type" == "release" ]; then
    build_dir="../build_release_lorc"
else
    echo "Invalid build type: $build_type. Please specify 'debug' or 'release'."
    exit 1
fi

# Use LD_LIBRARY_PATH instead of DYLD_LIBRARY_PATH on Linux
export LD_LIBRARY_PATH=$build_dir:$LD_LIBRARY_PATH

# Compilation command remains unchanged (adjust parameters as needed)
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
    # Set perf output filename
    profile_filename=${filename}${postfix:+_$postfix}

    # Use perf for performance sampling (requires root privileges or perf permissions)
    perf record -F 99 --call-graph dwarf -g -o ./profile/${profile_filename}.data ./bin/${filename}

    # Generate flame graph (FlameGraph tool needs to be installed)
    perf script -i ./profile/${profile_filename}.data | \
        stackcollapse-perf.pl | \
        flamegraph.pl > ./profile/${profile_filename}_linux_flamegraph.svg

    # Clean up intermediate files
    rm -f ./profile/${profile_filename}.data
fi

du -sh db/*db*
