/*
 * @Author: Li_diang 787695954@qq.com
 * @Date: 2023-04-16 19:37:32
 * @LastEditors: Li_diang 787695954@qq.com
 * @LastEditTime: 2023-04-20 12:01:52
 * @FilePath: \leveldb\include\leveldb\Remix.h
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
#ifndef STORAGE_LEVELDB_REMIX_SLICE_H_
#define STORAGE_LEVELDB_REMIX_SLICE_H_

#include <iostream>
#include <string>
#include <vector>

#include "leveldb/comparator.h"
#include "leveldb/db.h"
#include "leveldb/iterator.h"
#include "leveldb/slice.h"

using namespace std;
namespace leveldb {
struct Segment {
  vector<Iterator*>
      Cursor_Offsets;  // 每个段维护的迭代器，有N个run，就有N个Iterator*元素
  vector<int>
      Run_Selectors;  // run
                      // 选择器，每个run选择器对应一个键值对,该值指示现在下一个要访问的Cursor_Offsets的下标[0,...,n-1]
  vector<string> keys;  // 段里边存的键,与Run_Selecors一一对应
  size_t size;         // 当前段里实际存储的key的数量
  int runs_num;        // 当前共有n个段
  int key_num_perseg = 100;
  vector<int>runs_sum; // 当Run_Selectors[i] = n时，代表该键值对在第n个run中，此时需要复制Cursor_Offsets[n]这个迭代器，然后向后走runs_sum[n]个即可找到该键的迭代器
  vector<int> stept;
  Segment(int num)
      : Cursor_Offsets(num), runs_num(num), size(0), Run_Selectors(100),runs_sum(num,0) {
    // std::cout << "create segment successfully and create " << num
    //<< " Cursor_Offsets in it" << std::endl;
  }
  Segment() : size(0), Run_Selectors(20) {
    // std::cout << "create segment successfully" << std::endl;
  }
  ~Segment() {
    // cout << "delete segment" << endl;
  }
};
class Remix {
 public:
  std::vector<string> anchor_keys;  //锚键数组，每一个键对应一个segment
  std::vector<struct Segment> segments;
  size_t segment_size;      // 实际拥有的段数量
  size_t Max_segment_size;  // Remix中能够存放的最大段数
  int runs_num;             // run的数量
  DB* mydb;                 // DB指针，用于创建一个新的迭代器
  const Comparator*  cmp_;


  Remix(DB *db);
  

  Remix() { cout << "create a Remix successfully" << endl; }

  void print();
  
  void insert(string Internal_key, const Comparator* cmp, Iterator* iter);
 
  inline Slice ExtractUserKey(const Slice& internal_key) {
    assert(internal_key.size() >= 8);
    return Slice(internal_key.data(), internal_key.size() - 8);
  }

  inline bool empty();

  ~Remix() {
    // cout << "delete Remix" << endl;
  }
  
  Iterator* NewIterator();

 private:
  void insert_anchor_key(Iterator* iter, string Internal_key) {
    // cout << "insert the anchor key " <<
    // ExtractUserKey(iter->KEY()).ToString()
    //<< endl;
    anchor_keys[segment_size] =Internal_key;
    segments.push_back(Segment(runs_num));
    segment_size++;
    insert_to_segment(iter, segments[segment_size - 1],Internal_key);
  }

  bool insert_to_segment(Iterator* iter, Segment& dst,string Iternal_key) {
    if (dst.size + 1 > dst.key_num_perseg) {
      // cout << "the segment is full" << endl;
      return false;
    }
    // cout << "Segment: insert key " << ExtractUserKey(iter->KEY()).ToString()
    // << endl;
    dst.Run_Selectors[dst.size] = iter->get_index_of_runs();
    // cout << "insert the run_selector " << iter->get_index_of_runs() << endl;
    dst.keys.push_back(Iternal_key);
    dst.stept.push_back(dst.runs_sum[iter->get_index_of_runs()]);
    dst.runs_sum[iter->get_index_of_runs()] += 1;
    if (dst.Cursor_Offsets[iter->get_index_of_runs()] == NULL) {
      Iterator* temp = mydb->NewIterator(leveldb::ReadOptions());
      temp->Seek(iter->key());
      dst.Cursor_Offsets[iter->get_index_of_runs()] = temp;
      // cout << "insert a new iter to the segment " << endl;
    }
    dst.size++;
    return true;
  }
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_REMIX_SLICE_H_