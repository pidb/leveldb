#include <iostream>
#include <leveldb/db.h>

int main(int argc, char** argv) {
  if (argc < 2) {
    std::cout << "Usage: ./exam_01 <db_path>" << std::endl;
    return 1;
  }

  std::string db_path = argv[1];

  // 删除已存在的数据库
  leveldb::Options options;
  options.create_if_missing = true;
  leveldb::Status status = leveldb::DestroyDB(db_path, options);
  if (!status.ok()) {
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

  // 写入 10 个 key/value
  for (int i = 1; i <= 10; ++i) {
    std::string key = "key" + std::to_string(i);
    std::string value = "value" + std::to_string(i);
    status = db->Put(leveldb::WriteOptions(), key, value);
    if (!status.ok()) {
      std::cout << "Error writing to database: " << status.ToString()
                << std::endl;
      delete db;
      return 1;
    }
  }

  // 读取 10 个 key/value
  for (int i = 1; i <= 10; ++i) {
    std::string key = "key" + std::to_string(i);
    std::string value;
    status = db->Get(leveldb::ReadOptions(), key, &value);
    if (status.ok()) {
      std::cout << "Key: " << key << ", Value: " << value << std::endl;
    } else {
      std::cout << "Error reading from database: " << status.ToString()
                << std::endl;
      delete db;
      return 1;
    }
  }

  delete db;
  return 0;
}
