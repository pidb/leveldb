#include <iostream>
#include <leveldb/db.h>
#include <random>
#include <thread>

// 生成指定大小的随机字符串
static std::string generateRandomString(int size) {
  std::string chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
  std::string result;
  std::random_device rd;
  std::mt19937 generator(rd());
  std::uniform_int_distribution<int> distribution(0, chars.size() - 1);

  for (int i = 0; i < size; ++i) {
    result += chars[distribution(generator)];
  }

  return result;
}

int main(int argc, char** argv) {
  if (argc < 2) {
    std::cout << "Usage: ./exam_02 <db_path>" << std::endl;
    return 1;
  }

  std::string db_path = argv[1];

  // 删除已存在的数据库
  leveldb::Options options;
  options.create_if_missing = true;
  leveldb::Status status = leveldb::DestroyDB(db_path, options);
  if (!status.ok() && !status.IsNotFound()) {
    std::cout << "Error deleting existing database: " << status.ToString()
              << std::endl;
    return 1;
  }

  // 创建数据库
  leveldb::DB* db;
  status = leveldb::DB::Open(options, db_path, &db);
  if (!status.ok()) {
    std::cout << "Error opening database: " << status.ToString() << std::endl;
    return 1;
  }

  // 写入 4 个 key/value
  for (int i = 1; i <= 4; ++i) {
    std::string key = "key" + std::to_string(i);
    std::string value = generateRandomString(1024 * 1024);  // 1 MB 随机内容
    status = db->Put(leveldb::WriteOptions(), key, value);
    if (!status.ok()) {
      std::cout << "Error writing to database: " << status.ToString()
                << std::endl;
      delete db;
      return 1;
    }
  }

  // 此次写入会触发 minor compaction
  std::string key = "key" + std::to_string(5);
  std::string value = generateRandomString(1024 * 1024);  // 1 MB 随机内容
  status = db->Put(leveldb::WriteOptions(), key, value);
  if (!status.ok()) {
    std::cout << "Error writing to database: " << status.ToString()
              << std::endl;
    delete db;
    return 1;
  }

  while (true) {
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }
  // 关闭数据库
  delete db;
  return 0;
}
