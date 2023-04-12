/*
 * @Author: Li_diang 787695954@qq.com
 * @Date: 2023-04-04 22:59:17
 * @LastEditors: Li_diang 787695954@qq.com
 * @LastEditTime: 2023-04-12 10:23:54
 * @FilePath: \MYLevelDB\leveldb_test\leveldb_test.cpp
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
#include <iostream>
#include <vector>

#include "leveldb/db.h"
#include "leveldb/write_batch.h"
#define N 1000
using namespace std;
using namespace leveldb;
int main() {
  // 打开数据库
  leveldb::DB* mydb;
  // 初始化leveldb的memtable内存大小、文件操作接口、block大小、打开文件数限制等
  leveldb::Options myoptions;
  leveldb::Status mystatus;

  // 创建
  myoptions.create_if_missing = true;
  mystatus = leveldb::DB::Open(myoptions, "./testdb", &mydb);
  assert(mystatus.ok());

  // 写入数据
  /*#define N 4
  vector<string> keys;
  vector<string> values;
  for (int i = 0; i < N; i++) {
    keys.push_back("000" + to_string(i));
    values.push_back("####" + to_string(i));
  }
  for (int i = 0; mystatus.ok() && i < N; i++) {
    mydb->Put(WriteOptions(), keys[i], values[i]);
    assert(mystatus.ok());
  }*/

  // 批量写入
    /*for (int i = 0; i < N; i++) {
      std::string key = std::to_string(i);
      std::string value = std::to_string(i);
      mydb->Put(leveldb::WriteOptions(), key, value);
      assert(mystatus.ok());
    }*/

  // 按照顺序批量写入
  leveldb::WriteBatch batch;
  leveldb::Status s;
  for (int i = 0; i < N; i++) {
    std::string key = std::to_string(i);
    std::string value = std::to_string(i);
    // mydb->Put(leveldb::WriteOptions(), key, value);
    batch.Put(key, value);
  }
  s = mydb->Write(leveldb::WriteOptions(), &batch);
  assert(s.ok());
  

  // 读取数据
  // std::string key_ = "gonev";
  // std::string val_ = "";
  // mydb->Get(leveldb::ReadOptions(), key_, &val_);
  // std::cout << key_ << ": " << val_ << std::endl;

  // 遍历levelDB中的所有数据
   leveldb::Iterator* it = mydb->NewIterator(leveldb::ReadOptions());
   for (it->SeekToFirst(); it->Valid(); it->Next()) {
     std::cout << it->key().ToString() << ": " << it->value().ToString()
               << std::endl;
   }

  // 范围查询
  /*leveldb::Iterator* it = mydb->NewIterator(leveldb::ReadOptions());
  std::string start = "1";
  std::string limit = "123";
  for (it->Seek(start); it->Valid() && it->key().ToString() < limit;
       it->Next()) {
    std::cout << it->key().ToString() << ": " << it->value().ToString()
              << std::endl;
  }*/

  // 逆序遍历
  /*leveldb::Iterator* it = mydb->NewIterator(leveldb::ReadOptions());
  for (it->SeekToLast(); it->Valid(); it->Prev()) {
    std::cout << it->key().ToString() << ": " << it->value().ToString()
              << std::endl;
  }*/

  // 遍历测试
  /*leveldb::Iterator* it = mydb->NewIterator(leveldb::ReadOptions());
  if (it->Seek("1"), it->Valid()) {
    std::cout << it->key().ToString() << ": " << it->value().ToString()
              << std::endl;
  }
  if (it->Prev(), it->Valid()) {
    std::cout << it->key().ToString() << ": " << it->value().ToString()
              << std::endl;
  }
  if (it->Next(), it->Valid()) {
    std::cout << it->key().ToString() << ": " << it->value().ToString()
              << std::endl;
  }*/
}
