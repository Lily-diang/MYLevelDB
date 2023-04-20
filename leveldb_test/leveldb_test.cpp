#include <iostream>
#include <vector>

#include "leveldb/Remix.h"
#include "leveldb/db.h"
#include "leveldb/slice.h"
#include "leveldb/write_batch.h"
#define N 123
using namespace std;
using namespace leveldb;
int main() {
  /*
    Segment A;
    Remix B;
    cout << B.empty() << endl;
  */

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

  for (int i = 0; i < N; i++) {
    std::string key = std::to_string(i);
    std::string value = std::to_string(i);
    mydb->Put(leveldb::WriteOptions(), key, value);
    assert(mystatus.ok());
  }
  Remix* my_sorted_view = new Remix(mydb);
  //my_sorted_view->print();
  //cout << "create my sorted view successfully" << endl;
  Iterator* it = my_sorted_view->NewIterator();
  for (it->SeekToFirst(); it->Valid() && it->key().ToString() < "80"; it->Next()) {
    cout << it->key().ToString() << ": " << it->value().ToString() << endl;
  }
  // 按照顺序批量写入
  /*leveldb::WriteBatch batch;
  leveldb::Status s;
  for (int i = 0; i < N; i++) {
    std::string key = std::to_string(i);
    std::string value = std::to_string(i);
    // mydb->Put(leveldb::WriteOptions(), key, value);
    batch.Put(key, value);
  }

  s = mydb->Write(leveldb::WriteOptions(), &batch);
  assert(s.ok());*/

  // 读取数据
  // std::string key_ = "gonev";
  // std::string val_ = "";
  // mydb->Get(leveldb::ReadOptions(), key_, &val_);
  // std::cout << key_ << ": " << val_ << std::endl;

  // 遍历levelDB中的所有数据
  /*leveldb::Iterator* it = mydb->NewIterator(leveldb::ReadOptions());
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    if (it->get_runs_num() > 1)
    std::cout << it->value().ToString() << std::endl;
    //cout << endl;
  }*/

  // 范围查询
  /*
  leveldb::Iterator* it = mydb->NewIterator(leveldb::ReadOptions());
  std::string start = "97800";
  std::string limit = "97810";
  for (it->Seek(start); it->Valid() && it->key().ToString() < limit;
       it->Next()) {
    std::cout << it->key().ToString() << ": "
        << it->value().ToString()
              << std::endl;
  }

  start = "10000";
  limit = "10010";
  for (it->Seek(start); it->Valid() && it->key().ToString() < limit;
       it->Next()) {
    std::cout << it->key().ToString() << ": " << it->value().ToString()
              << std::endl;
  }
  */
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
