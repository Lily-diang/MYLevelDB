/*
 * @Author: Li_diang 787695954@qq.com
 * @Date: 2023-04-20 10:58:18
 * @LastEditors: Li_diang 787695954@qq.com
 * @LastEditTime: 2023-04-21 12:28:40
 * @FilePath: \leveldb\db\Remix.cc
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
#include "leveldb/Remix.h"
#include "leveldb/RemixIterator.h"
using namespace std;
namespace leveldb{
Remix::Remix(DB* db)
      : segment_size(0), Max_segment_size(5), mydb(db),key_num_perseg(5) {
    //cout << "create the Remix successfully" << endl;
    anchor_keys = new string[5];
    segments = new struct Segment[5];
    //segmens = new struct Segment[5](runs_num,key_num_perseg);
    ReadOptions opt;
    Iterator* it = db->NewIterator(opt);
    //cout << "create the it successfully and the runs_num is "
         //<< it->get_runs_num() << endl;
    runs_num = it->get_runs_num();
    cmp_ = it->get_comparator();
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
      //if (it->key().ToString() > "190000") {
      //cout << it->value().ToString() << endl;
      //}
      char *temp1 = (char *)malloc(it->key().size());
      memcpy(temp1,it->key().data(),it->key().size());
      insert(string(temp1,it->key().size()), cmp_, it);
    }
    // printf("create Remix successfully\n");
  }
  Remix::Remix(DB* db,int num)
      : segment_size(0), Max_segment_size(5), mydb(db),key_num_perseg(num) {
    //cout << "create the Remix successfully" << endl;
    anchor_keys = new string[5];
    segments = new struct Segment[5];
    ReadOptions opt;
    Iterator* it = db->NewIterator(opt);
    //cout << "create the it successfully and the runs_num is "
         //<< it->get_runs_num() << endl;
    runs_num = it->get_runs_num();
    cmp_ = it->get_comparator();
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
      //if (it->key().ToString() > "190000") {
      //cout << it->value().ToString() << endl;
      //}
      char *temp1 = (char *)malloc(it->key().size());
      memcpy(temp1,it->key().data(),it->key().size());
      insert(string(temp1,it->key().size()), cmp_, it);
    }
    // printf("create Remix successfully\n");
  }

  void Remix::print() {
    cout << endl;
    cout << "the segment array: " << endl;
    for (size_t i = 0; i < segment_size; i++) {
      cout << anchor_keys[i] << " ";
    }
    cout << endl;
    cout << "the keys in the Remix :" << endl;
    for (size_t i = 0; i < segment_size; i++) {
      for (int j = 0; j < segments[i].size; j++) {
        cout << (segments[i].keys[j]) << " ";
      }
      cout << endl;
    }
    cout << endl;
  }

   void Remix::insert(string Internal_key, const Comparator* cmp, Iterator* iter) {
    //cout << "insert:" << iter->key().ToString() << endl;
    //cout << "insert the Internal_key :" << Internal_key << endl; 
    //print();
    // 长度不够
    if (segment_size + 1 > Max_segment_size) {
      //anchor_keys.resize(Max_segment_size + 5);
      string* temp = new string[Max_segment_size+5];
      struct Segment *tempp = new struct Segment[Max_segment_size+5];
      for(int i = 0; i < Max_segment_size; i++){
        temp[i] = anchor_keys[i];
        tempp[i] = segments[i];
      }
      delete []anchor_keys;
      delete []segments;
      anchor_keys = temp;
      segments = tempp;
      Max_segment_size += 5;
    }

    if (segment_size == 0) {  // 第一个节点
      insert_anchor_key(iter, Internal_key);
      // print();

    } else {
      size_t left = 0, right = segment_size - 1, mid;
      while (left < right) {
        mid = (left + right) / 2;
        if (anchor_keys[mid]<= Internal_key) {
          left = mid + 1;
        } else {
          right = mid;
        }
      }
      if (left == right && left == segment_size - 1) {
        // 只有一个值
        if (anchor_keys[left]< Internal_key) {
          if (insert_to_segment(iter, segments[left],Internal_key) ==
              false) {  // 这个segment已经满了,未插入成功
            insert_anchor_key(iter, Internal_key);
          }
        } else {
          insert_anchor_key(iter, Internal_key);
          swap(anchor_keys[left], anchor_keys[left + 1]);
          swap(segments[left], segments[left + 1]);
        }
      } else {
        if (anchor_keys[left]<Internal_key &&
            insert_to_segment(iter, segments[left],Internal_key) == true) {
          // 成功插入
        } else {
          insert_anchor_key(iter, Internal_key);
          for (size_t i = segment_size - 1; i > left; i--) {
            swap(anchor_keys[i], anchor_keys[i - 1]);
            swap(segments[i], segments[i - 1]);
          }
        }
      }
    }
    //if (iter->get_index_of_runs() > 1) 
    //print();
  }

  inline bool Remix::empty() {
    if (segment_size == 0) {
      std::cout << "this Remix is empty" << std::endl;
      return true;
    } else
      return false;
  }


    Iterator* Remix::NewIterator(){
    
     return new RemixIterator(this);
  }
}