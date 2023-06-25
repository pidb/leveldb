# 添加 LevelDB 库的路径
set(LEVELDB_DIR ${PROJECT_SOURCE_DIR})

# 添加 LevelDB 头文件的路径
include_directories(${LEVELDB_DIR}/include)

# 添加 LevelDB 库文件的路径
link_directories(${LEVELDB_DIR}/build)

set(LEVELDB_EXAMPLE ${PROJECT_SOURCE_DIR}/examples)

add_executable(exam_01 ${LEVELDB_EXAMPLE}/exam_01.cpp)

# 链接 LevelDB 库
target_link_libraries(exam_01 leveldb)
