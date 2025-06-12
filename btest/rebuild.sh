#!/bin/bash

build_type=${1:-"release"}
mode=${2:-"build"} # build / make
with_java=${3:-"no_java"}

with_jni=0

if [ "$build_type" == "debug" ]; then
    build_dir="../build_debug_lorc"
    cmake_build_type="Debug"
elif [ "$build_type" == "release" ]; then
    build_dir="../build_release_lorc"
    cmake_build_type="Release"

else
    echo "Invalid build type. Please specify 'debug' or 'release'."
    exit 1
fi

if [ "$with_java" == "with_java" ]; then
    with_jni=1
    echo "Building with Java support."
elif [ "$with_java" == "no_java" ]; then
    with_jni=0
    echo "Building without Java support."
else
    echo "Invalid Java option. Please specify 'with_java' or 'no_java'."
    exit 1
fi

if [ "$mode" == "build" ]; then
    # Check if the build directory exists, if it does, delete it first
    if [ -d "$build_dir" ]; then
        echo "Directory $build_dir already exists. Deleting it..."
        rm -rf "$build_dir"
    fi
    # Create a new build directory
    echo "Creating directory $build_dir..."
    mkdir -p "$build_dir"
    # Enter the build directory and run CMake and Make
    cd "$build_dir" || exit 1
    echo "Building..."
    sudo rm -rf /home/gjr/mylibs/lorcdb_$build_type
    sudo cmake -DWITH_JEMALLOC=0 -DWITH_SNAPPY=1 -DWITH_LZ4=1 -DWITH_ZLIB=1 -DCMAKE_BUILD_TYPE=$cmake_build_type -DWITH_GFLAGS=1 -DWITH_JNI=$with_jni \
            -DCMAKE_INSTALL_PREFIX=/home/gjr/mylibs/lorcdb_$build_type \
            -DJAVA_HOME=/home/tx/jdk1.8.0_401 \
            -DJAVA_INCLUDE_PATH=/home/tx/jdk1.8.0_401/include \
            -DJAVA_INCLUDE_PATH2=/home/tx/jdk1.8.0_401/include/linux \
            -DJAVA_JVM_LIBRARY=/home/tx/jdk1.8.0_401/jre/lib/amd64/server/libjvm.so \
            ..
    sudo make -j
    sudo make install
elif [ "$mode" == "make" ]; then
    # Check if the build directory exists, if not, report an error
    if [ ! -d "$build_dir" ]; then
        echo "Directory $build_dir does not exist. Please run with 'build' mode first."
        exit 1
    fi
    cd "$build_dir" || exit 1
    echo "Making..."
    sudo rm -rf /home/gjr/mylibs/lorcdb_$build_type
    sudo make -j
    sudo make install
else
    echo "Invalid mode. Please specify 'build' or 'make'."
    exit 1
fi

cd ..