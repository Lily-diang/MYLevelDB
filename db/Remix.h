/*
 * @Author: Li_diang 787695954@qq.com
 * @Date: 2023-04-16 19:37:32
 * @LastEditors: Li_diang 787695954@qq.com
 * @LastEditTime: 2023-04-19 10:58:46
 * @FilePath: \leveldb\db\Remix.h
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置
 * 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
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
  vector<Slice> keys;  // 段里边存的键,与Run_Selecors一一对应
  size_t size;         // 当前段里实际存储的key的数量
  int runs_num;        // 当前共有n个段
  int key_num_perseg = 20;

  Segment(int num)
      : Cursor_Offsets(num), runs_num(num), size(0), Run_Selectors(20) {
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
  std::vector<Slice> anchor_keys;  //锚键数组，每一个键对应一个segment
  std::vector<struct Segment> segments;
  size_t segment_size;      // 实际拥有的段数量
  size_t Max_segment_size;  // Remix中能够存放的最大段数
  int runs_num;             // run的数量
  DB* mydb;                 // DB指针，用于创建一个新的迭代器

  Remix(DB* db, int num)
      : segment_size(0),
        Max_segment_size(5),
        anchor_keys(5),
        runs_num(num),
        mydb(db) {
    // printf("create Remix successfully\n");
  }

  Remix(DB* db)
      : segment_size(0), Max_segment_size(5), anchor_keys(5), mydb(db) {
    cout << "create the Remix successfully" << endl;
    Iterator* it = db->NewIterator(leveldb::ReadOptions());
    cout << "create the it successfully and the runs_num is "
         << it->get_runs_num() << endl;
    runs_num = it->get_runs_num();
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
      insert(it->KEY(), it->get_comparator(), it);
    }
    // printf("create Remix successfully\n");
  }

  Remix() { cout << "create a Remix successfully" << endl; }

  void print() {
    cout << endl;
    cout << "the segment array: " << endl;
    for (size_t i = 0; i < segment_size; i++) {
      cout << ExtractUserKey(anchor_keys[i]).ToString() << " ";
    }
    cout << endl;
    cout << "the keys in the Remix :" << endl;
    for (size_t i = 0; i < segment_size; i++) {
      for (int j = 0; j < segments[i].size; j++) {
        cout << ExtractUserKey(segments[i].keys[j]).ToString() << " ";
      }
      cout << endl;
    }
    cout << endl;
  }

  void insert(Slice key, const Comparator* cmp, Iterator* iter) {
    //cout << iter->key().ToString() << " ";
            // 长度不够
            if (segment_size + 1 > Max_segment_size) {
      anchor_keys.resize(Max_segment_size + 5);
      Max_segment_size += 5;
    }

    if (segment_size == 0) {  // 第一个节点
      insert_anchor_key(iter, key);
      // print();

    } else {
      size_t left = 0, right = segment_size - 1, mid;
      while (left < right) {
        mid = (left + right) / 2;
        if (cmp->Compare(anchor_keys[mid], key) <= 0) {
          left = mid + 1;
        } else {
          right = mid;
        }
      }
      if (left == right && left == segment_size - 1) {
        // 只有一个值
        if (cmp->Compare(anchor_keys[left], key) < 0) {
          if (insert_to_segment(iter, segments[left]) ==
              false) {  // 这个segment已经满了,未插入成功
            insert_anchor_key(iter, key);
          }
        } else {
          insert_anchor_key(iter, key);
          swap(anchor_keys[left], anchor_keys[left + 1]);
          swap(segments[left], segments[left + 1]);
        }
      } else {
        if (cmp->Compare(anchor_keys[left], key) < 0 &&
            insert_to_segment(iter, segments[left]) == true) {
          // 成功插入
        } else {
          insert_anchor_key(iter, key);
          for (size_t i = segment_size - 1; i > left; i--) {
            swap(anchor_keys[i], anchor_keys[i - 1]);
            swap(segments[i], segments[i - 1]);
          }
        }
      }
    }
    if (iter->get_index_of_runs() > 1) print();
  }

  inline Slice ExtractUserKey(const Slice& internal_key) {
    assert(internal_key.size() >= 8);
    return Slice(internal_key.data(), internal_key.size() - 8);
  }

  inline bool empty() {
    if (segment_size == 0) {
      std::cout << "this Remix is empty" << std::endl;
      return true;
    } else
      return false;
  }

  ~Remix() {
    // cout << "delete Remix" << endl;
  }

 private:
  void insert_anchor_key(Iterator* iter, Slice key) {
    // cout << "insert the anchor key " <<
    // ExtractUserKey(iter->KEY()).ToString()
    //<< endl;
    anchor_keys[segment_size] = key;
    segments.push_back(Segment(runs_num));
    segment_size++;
    insert_to_segment(iter, segments[segment_size - 1]);
  }

  bool insert_to_segment(Iterator* iter, Segment& dst) {
    if (dst.size + 1 > dst.key_num_perseg) {
      // cout << "the segment is full" << endl;
      return false;
    }
    // cout << "Segment: insert key " << ExtractUserKey(iter->KEY()).ToString()
    // << endl;
    dst.Run_Selectors[dst.size] = iter->get_index_of_runs();
    // cout << "insert the run_selector " << iter->get_index_of_runs() << endl;
    dst.keys.push_back(iter->KEY());
    if (dst.Cursor_Offsets[iter->get_index_of_runs()] == NULL) {
      Iterator* temp = mydb->NewIterator(leveldb::ReadOptions());
      temp->Seek(iter->KEY());
      dst.Cursor_Offsets[iter->get_index_of_runs()] = temp;
      // cout << "insert a new iter to the segment " << endl;
    }
    dst.size++;
    return true;
  }
};

class RemixIterator : public Iterator {
 public:
  RemixIterator() {}

  ~RemixIterator() override;
  void Seek(const Slice& target) override;
  int SeekToFirst() override;
  void SeekToLast() override;
  int Next() override;
  void Prev() override;
  bool Valid() const override { return current_->Valid(); }
  Slice key() const override {
    assert(Valid());
    return current_->key();
  }
  Slice value() const override {
    assert(Valid());
    return current_->value();
  }
  Status status() const override { return current_->status(); }

 private:
  Iterator* current_;
  Remix my_sorted_view_;
};

}  // namespace leveldb
