filename=$1
mode=${2:-"debug"}

if [ "$mode" == "debug" ]; then
    build_dir="../build_debug"
elif [ "$mode" == "release" ]; then
    build_dir="../build_release"
else
    echo "Invalid mode. Please specify 'debug' or 'release'."
    exit 1
fi

export DYLD_LIBRARY_PATH=$build_dir:$DYLD_LIBRARY_PATH

g++ -std=c++17 -I../include -L$build_dir -o ./bin/${filename} ./code/${filename}.cc -lrocksdb -lpthread -lz -lbz2

./bin/${filename}