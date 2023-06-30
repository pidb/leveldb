#include <iostream>
#include <leveldb/db.h>
#include <random>
#include <thread>
#include <vector>

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

static std::string Key(int i) {
  char buf[100];
  std::snprintf(buf, sizeof(buf), "key%06d", i);
  return std::string(buf);
}

static leveldb::Status TryReopen(leveldb::Options* options, std::string& dbname,
                                 leveldb::DB* db) {
  leveldb::Options opts;
  if (options != nullptr) {
    opts = *options;
  } else {
    opts = leveldb::Options();
    opts.create_if_missing = true;
  }

  return leveldb::DB::Open(opts, dbname, &db);
}

static leveldb::Status ReOpen(leveldb::Options* options, std::string& dbname,
                              leveldb::DB* new_db) {
  return TryReopen(options, dbname, new_db);
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
  options.write_buffer_size = 100000000;  // 设置一个非常大的 buffer
  status = leveldb::DB::Open(options, db_path, &db);
  if (!status.ok()) {
    std::cout << "Error opening database: " << status.ToString() << std::endl;
    return 1;
  }

  // 写 ~4MB (41 values, 每个 100k)
  for (int i = 2; i < 43; i++) {
    status =
        db->Put(leveldb::WriteOptions(), Key(i), generateRandomString(100000));
    if (!status.ok()) {
      std::cout << "Error writing to database: " << status.ToString()
                << std::endl;
      delete db;
      return 1;
    }
  }

  for (int i = 15; i < 56; i++) {
    status =
        db->Put(leveldb::WriteOptions(), Key(i), generateRandomString(100000));
    if (!status.ok()) {
      std::cout << "Error writing to database: " << status.ToString()
                << std::endl;
      delete db;
      return 1;
    }
  }

  for (int i = 0; i < 42; i++) {
    status =
        db->Put(leveldb::WriteOptions(), Key(i), generateRandomString(100000));
    if (!status.ok()) {
      std::cout << "Error writing to database: " << status.ToString()
                << std::endl;
      delete db;
      return 1;
    }
  }

  for (int i = 43; i < 84; i++) {
    status =
        db->Put(leveldb::WriteOptions(), Key(i), generateRandomString(100000));
    if (!status.ok()) {
      std::cout << "Error writing to database: " << status.ToString()
                << std::endl;
      delete db;
      return 1;
    }
  }

  // 重新打开数据库, 移动更新到 level-0
  delete db;
  options = leveldb::Options();
  status = ReOpen(&options, db_path, db);
  if (!status.ok()) {
    std::cout << "Reopen error: " << status.ToString() << std::endl;
    delete db;
    return 1;
  }

  // 此次写入会触发 level-0 majoy compaction
  status = db->Put(leveldb::WriteOptions(), Key(81),
                   generateRandomString(1024 * 1024));
  if (!status.ok()) {
    std::cout << "Error writing to database: " << status.ToString()
              << std::endl;
    delete db;
    return 1;
  }

  // 此次写入会触发 minor compaction
  //   std::string key = "key" + std::to_string(5);
  //   std::string value = generateRandomString(1024 * 1024);  // 1 MB 随机内容
  //   status = db->Put(leveldb::WriteOptions(), key, value);
  //   if (!status.ok()) {
  //     std::cout << "Error writing to database: " << status.ToString()
  //               << std::endl;
  //     delete db;
  //     return 1;
  //   }

  while (true) {
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }
  // 关闭数据库
  delete db;
  return 0;
}
