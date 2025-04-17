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
sudo cmake -DWITH_JEMALLOC=0 -DWITH_SNAPPY=1 -DWITH_LZ4=1 -DWITH_ZLIB=1 -DCMAKE_BUILD_TYPE=Release -DWITH_GFLAGS=1 -DWITH_JNI=1 \
         -DJAVA_HOME=/home/tx/jdk1.8.0_401 \
         -DJAVA_INCLUDE_PATH=/home/tx/jdk1.8.0_401/include \
         -DJAVA_INCLUDE_PATH2=/home/tx/jdk1.8.0_401/include/linux \
         -DJAVA_JVM_LIBRARY=/home/tx/jdk1.8.0_401/jre/lib/amd64/server/libjvm.so \
         ..
sudo make -j
cd ..