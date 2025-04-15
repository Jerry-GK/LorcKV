#!/bin/bash

mode=${1:-"release"}

if [ "$mode" == "debug" ]; then
    build_dir="../build_debug"
elif [ "$mode" == "release" ]; then
    build_dir="../build_release"
else
    echo "Invalid mode. Please specify 'debug' or 'release'."
    exit 1
fi

# 检查构建目录是否存在
if [ -d "$build_dir" ]; then
    echo "Directory $build_dir already exists. Deleting it..."
    rm -rf "$build_dir"
fi

# 创建新的构建目录
echo "Creating directory $build_dir..."
mkdir -p "$build_dir"

# 进入构建目录并运行 CMake 和 Make
cd "$build_dir" || exit 1
cmake -DCMAKE_BUILD_TYPE=Release -DWITH_GFLAGS=1 -DWITH_SNAPPY=1 ..
make -j
cd ..