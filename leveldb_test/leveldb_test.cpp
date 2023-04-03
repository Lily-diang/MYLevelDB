#include <iostream>
#include "leveldb/write_batch.h"
#include "leveldb/db.h"
#define N 1000
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
  std::string key = "gonev";
  std::string value = "a handsome man";
  std::string key2 = "0001";
  std::string value2 = "0000001";

  if (mystatus.ok()) {
    mydb->Put(leveldb::WriteOptions(), key, value);
    mydb->Put(leveldb::WriteOptions(), key2, value2);
    assert(mystatus.ok());
  }

  // 批量写入
/*  for (int i = 0; i < N; i++) {
    std::string key = std::to_string(i);
    std::string value = std::to_string(i) + "1";
    mydb->Put(leveldb::WriteOptions(), key, value);
    assert(mystatus.ok());
  }*/

  // 按照顺序批量写入
  leveldb::WriteBatch batch;
  leveldb::Status s;
  for (int i = 0; i < N; i++) {
    std::string key = std::to_string(i);
    std::string value = std::to_string(i) + "1";
    //mydb->Put(leveldb::WriteOptions(), key, value);
    batch.Put(key, value);
  }
  s = mydb->Write(leveldb::WriteOptions(), &batch);
  assert(s.ok());

  // 读取数据
  std::string key_ = "gonev";
  std::string val_ = "";
  mydb->Get(leveldb::ReadOptions(), key_, &val_);
  std::cout << key_ << ": " << val_ << std::endl;
  
  
  // 遍历levelDB中的所有数据
  /*leveldb::Iterator* it = mydb->NewIterator(leveldb::ReadOptions());
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    std::cout << it->key().ToString() << ": " << it->value().ToString()
              << std::endl;
  }*/

  // 范围查询
/*  leveldb::Iterator* it = mydb->NewIterator(leveldb::ReadOptions());
  std::string start = "1";
  std::string limit = "123";
  for (it->Seek(start); it->Valid() && it->key().ToString() < limit;
       it->Next()) {
    std::cout << it->key().ToString() << ": " << it->value().ToString() << std::endl;
  }*/
}
