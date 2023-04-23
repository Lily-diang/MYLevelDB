/*
 * @Author: Li_diang 787695954@qq.com
 * @Date: 2023-04-16 19:37:32
 * @LastEditors: Li_diang 787695954@qq.com
 * @LastEditTime: 2023-04-23 11:15:41
 * @FilePath: \leveldb\include\leveldb\Remix.h
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
#ifndef STORAGE_LEVELDB_REMIX_SLICE_H_
#define STORAGE_LEVELDB_REMIX_SLICE_H_

#include <iostream>
#include <string>
#include <vector>
#include <algorithm>

#include "leveldb/comparator.h"
#include "leveldb/db.h"
#include "leveldb/iterator.h"
#include "leveldb/slice.h"

using namespace std;
namespace leveldb {
struct Segment {
  Iterator**
      Cursor_Offsets;  // 每个段维护的迭代器，有N个run，就有N个Iterator*元素
  Iterator**
      Cursor_;
  //vector<int>
  int *
      Run_Selectors;  // run
                      // 选择器，每个run选择器对应一个键值对,该值指示现在下一个要访问的Cursor_Offsets的下标[0,...,n-1]
  //vector<string> keys;  // 段里边存的键,与Run_Selecors一一对应
  string * keys;
  size_t size;         // 当前段里实际存储的key的数量
  int runs_num;        // 当前共有n个段
  int key_num_perseg;
  int keys_capacity;
  int * runs_sum;
  //vector<int> runs_sum; // 当Run_Selectors[i] = n时，代表该键值对在第n个run中，此时需要复制Cursor_Offsets[n]这个迭代器，然后向后走runs_sum[n]个即可找到该键的迭代器
  vector<int> stept;
  // Segment(int num)
  //     : Cursor_Offsets(num), runs_num(num), size(0), Run_Selectors(20),runs_sum(num,0) {
  //   // std::cout << "create segment successfully and create " << num
  //   //<< " Cursor_Offsets in it" << std::endl;
  // }
    Segment(int num)
      :  runs_num(num), size(0),key_num_perseg(5) ,keys_capacity(10){
    // std::cout << "create segment successfully and create " << num
    //<< " Cursor_Offsets in it" << std::endl;
    Cursor_Offsets = new Iterator *[5];
     Run_Selectors = new int[5]();
     runs_sum = new int[5]();
     keys = new string[10];
     Cursor_ = new Iterator*[num];
  }
     Segment(int num, int pre_num)
      :  runs_num(num), size(0),key_num_perseg(pre_num),keys_capacity(10){
    // std::cout << "create segment successfully and create " << num
    //<< " Cursor_Offsets in it" << std::endl;
    Cursor_Offsets = new Iterator *[pre_num];
    Run_Selectors = new int [pre_num]();
    runs_sum = new int[pre_num]();
    keys = new string[10];
    Cursor_ = new Iterator *[num];
  }
  Segment(){}

  ~Segment() {
    // cout << "delete segment" << endl;
    //delete[] Cursor_Offsets;
    //delete[] Run_Selectors;
    //delete[] runs_sum;
  }
};
class Remix {
 public:
  //std::vector<string> anchor_keys;  //锚键数组，每一个键对应一个segment
  string *anchor_keys;
  //std::vector<struct Segment> segments;
  struct Segment* segments;
  size_t segment_size;      // 实际拥有的段数量
  size_t Max_segment_size;  // Remix中能够存放的最大段数
  int runs_num;             // run的数量
  DB* mydb;                 // DB指针，用于创建一个新的迭代器
  const Comparator*  cmp_;
  int key_num_perseg;


  Remix(DB *db);
  Remix(DB *db,int num);
  

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
    segments[segment_size] = Segment(runs_num,key_num_perseg);
    segment_size++;
    insert_to_segment(iter, segments[segment_size - 1],Internal_key);
  }

  bool insert_to_segment(Iterator* iter, Segment& dst,string Iternal_key) {
    if (dst.size + 1 > dst.key_num_perseg) {
      // cout << "the segment is full" << endl;
      return false;
    }
    if(dst.size + 1 > dst.keys_capacity){
      string* temp = new string[dst.keys_capacity+10];
      for(int i = 0; i < dst.keys_capacity; i++){
        temp[i] = dst.keys[i];
      }
      delete []dst.keys;
      dst.keys = temp;
      dst.keys_capacity += 10;
    }
    // cout << "Segment: insert key " << ExtractUserKey(iter->KEY()).ToString()
    // << endl;
    dst.Run_Selectors[dst.size] = iter->get_index_of_runs();
    // cout << "insert the run_selector " << iter->get_index_of_runs() << endl;
    dst.keys[dst.size] = Iternal_key;
    dst.stept.push_back(dst.runs_sum[iter->get_index_of_runs()]);
    dst.runs_sum[iter->get_index_of_runs()] += 1;
    if (dst.Cursor_[iter->get_index_of_runs()] == NULL) {
      Iterator* temp = mydb->NewIterator(leveldb::ReadOptions());
      temp->Seek(iter->key());
      dst.Cursor_[iter->get_index_of_runs()] = temp;
      // cout << "insert a new iter to the segment " << endl;
    }
    dst.Cursor_Offsets[dst.size] = mydb->NewIterator(leveldb::ReadOptions());
    dst.Cursor_Offsets[dst.size]->Seek(iter->key());
    dst.size++;
    return true;
  }
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_REMIX_SLICE_H_
