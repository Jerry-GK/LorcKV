filename=$1
mode=${2:-"release"}
postfix=${3:-""}

profile_time=300

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

# 在后台运行程序并获取PID
./bin/${filename} &
pid=$!

# 等待程序启动
# sleep 1

# 使用sample工具进行性能分析
profile_filename=${filename}${postfix:+_$postfix}
sample $pid $profile_time -f ./profile/${profile_filename}.prof

# 生成火焰图
stackcollapse-sample.awk ./profile/${profile_filename}.prof | flamegraph.pl > ./profile/${profile_filename}_flamegraph.svg

# 清理
rm -f ./profile/${profile_filename}.prof