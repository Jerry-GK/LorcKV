# 编译器设置
CXX = g++
CXXFLAGS = -std=c++17 -g -O2 -fno-omit-frame-pointer -rdynamic

# 目标文件
TARGET = ./bin/testVecLORC

# 源文件目录
SRC_DIRS = . cache range iterator logger storage

# 自动找出所有的源文件
SRCS = $(foreach dir,$(SRC_DIRS),$(wildcard $(dir)/*.cc))

# 生成目标文件列表
OBJS = $(SRCS:.cc=.o)

# 确保bin目录存在
$(shell mkdir -p bin)

# 默认目标
all: $(TARGET)

# 链接规则
$(TARGET): $(OBJS)
	$(CXX) $(OBJS) -o $(TARGET) $(CXXFLAGS)

# 编译规则
%.o: %.cc
	$(CXX) $(CXXFLAGS) -c $< -o $@

# 清理规则
cleanObjects:
	rm -f $(OBJS)

cleanTarget:
	rm -f $(TARGET)

clean:
	rm -f $(OBJS) $(TARGET)

# 声明伪目标
.PHONY: all clean