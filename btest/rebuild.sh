#!/bin/bash

mode=${1:-"release"}

if [ "$mode" == "debug" ]; then
    build_dir="../build_debug_lorc"
elif [ "$mode" == "release" ]; then
    build_dir="../build_release_lorc"
else
    echo "Invalid mode. Please specify 'debug' or 'release'."
    exit 1
fi

# ��鹹��Ŀ¼�Ƿ����
if [ -d "$build_dir" ]; then
    echo "Directory $build_dir already exists. Deleting it..."
    rm -rf "$build_dir"
fi

# �����µĹ���Ŀ¼
echo "Creating directory $build_dir..."
mkdir -p "$build_dir"

# ���빹��Ŀ¼������ CMake �� Make
cd "$build_dir" || exit 1
sudo cmake -DWITH_JEMALLOC=0 -DWITH_SNAPPY=1 -DWITH_LZ4=1 -DWITH_ZLIB=1 -DCMAKE_BUILD_TYPE=Release -DWITH_GFLAGS=1 -DWITH_JNI=1 \
         -DJAVA_HOME=/home/tx/jdk1.8.0_401 \
         -DJAVA_INCLUDE_PATH=/home/tx/jdk1.8.0_401/include \
         -DJAVA_INCLUDE_PATH2=/home/tx/jdk1.8.0_401/include/linux \
         -DJAVA_JVM_LIBRARY=/home/tx/jdk1.8.0_401/jre/lib/amd64/server/libjvm.so \
         ..
sudo make -j
cd ..