filename=$1
mode=${2:-"release"}

if [ "$mode" == "debug" ]; then
    build_dir="../build_debug"
elif [ "$mode" == "release" ]; then
    build_dir="../build_release"
else
    echo "Invalid mode. Please specify 'debug' or 'release'."
    exit 1
fi

export LD_LIBRARY_PATH=$build_dir:$LD_LIBRARY_PATH

g++ -std=c++17 -I../include -L$build_dir -o ./bin/${filename} ./code/${filename}.cc -lrocksdb -lpthread -lz -lbz2

./bin/${filename}

du -sh db/test_db