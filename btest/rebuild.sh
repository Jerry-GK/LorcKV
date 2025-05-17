#!/bin/bash

build_type=${1:-"release"}
mode=${2:-"build"} # build / make

if [ "$build_type" == "debug" ]; then
    build_dir="../build_debug_lorc"
elif [ "$build_type" == "release" ]; then
    build_dir="../build_release_lorc"
else
    echo "Invalid build type. Please specify 'debug' or 'release'."
    exit 1
fi

if [ "$mode" == "build" ]; then
    # 检查构建目录是否存在，存在就删除重建
    if [ -d "$build_dir" ]; then
        echo "Directory $build_dir already exists. Deleting it..."
        rm -rf "$build_dir"
        # 创建新的构建目录
        echo "Creating directory $build_dir..."
        mkdir -p "$build_dir"
        # 进入构建目录并运行 CMake 和 Make
        cd "$build_dir" || exit 1
        echo "Building..."
        sudo cmake -DWITH_JEMALLOC=0 -DWITH_SNAPPY=1 -DWITH_LZ4=1 -DWITH_ZLIB=1 -DCMAKE_BUILD_TYPE=Release -DWITH_GFLAGS=1 -DWITH_JNI=0 \
                -DJAVA_HOME=/home/tx/jdk1.8.0_401 \
                -DJAVA_INCLUDE_PATH=/home/tx/jdk1.8.0_401/include \
                -DJAVA_INCLUDE_PATH2=/home/tx/jdk1.8.0_401/include/linux \
                -DJAVA_JVM_LIBRARY=/home/tx/jdk1.8.0_401/jre/lib/amd64/server/libjvm.so \
                ..
        sudo make -j
    fi
elif [ "$mode" == "make" ]; then
    # 检查构建目录是否存在，不存在就报错
    if [ ! -d "$build_dir" ]; then
        echo "Directory $build_dir does not exist. Please run with 'build' mode first."
        exit 1
    fi
    cd "$build_dir" || exit 1
    echo "Making..."
    sudo make -j
else
    echo "Invalid mode. Please specify 'build' or 'make'."
    exit 1
fi

cd ..