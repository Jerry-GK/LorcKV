# compile 
g++ -std=c++17 -g -O2 *.cc -o ./bin/testLORC 

# run
# ./bin/testLORC

# profile
perf record -F 99 -g -o ./profile/profile.data ./bin/testLORC

# 生成火焰图（需提前安装FlameGraph工具）
perf script -i ./profile/profile.data | \
    stackcollapse-perf.pl | \
    flamegraph.pl > ./profile/lorc_flamegraph.svg

# clean
rm profile/profile.data
rm bin/testLORC