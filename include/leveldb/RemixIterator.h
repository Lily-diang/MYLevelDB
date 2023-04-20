
#include <iostream>
#include <string>
#include <vector>

#include "leveldb/iterator.h"
#include "leveldb/Remix.h"
using namespace std;

namespace leveldb{
    
class RemixIterator : public Iterator {
 public:
  RemixIterator(Remix sorted_view) : current_(),my_sorted_view_(sorted_view){cout << "Create a RemixIterator successfully " << endl;}

  ~RemixIterator() override {if(current_ != NULL) delete current_;};
  void Seek(const Slice& target) override{
    string targ = target.ToString();
    size_t left = 0, right = my_sorted_view_.segment_size-1,mid;
    while(left < right){
      mid = (left+right) /2;
      if(my_sorted_view_.anchor_keys[mid] < targ){
        left = mid + 1;
      }
      else {
        right = mid;
      }
    }
    if (my_sorted_view_.anchor_keys[left] > targ) left--;
    // 再段里进行二分查找，找到第一个大于或等于targ的位置，然后向后移
    Segment seg = my_sorted_view_.segments[left];
    vector<string> keys = seg.keys;
    size_t l = 0, r = seg.size-1;
    while(l < r){
        mid = (l+r) /2;
        if(keys[mid] < targ){
            l = mid+1;
        }
        else{
            r = mid;
        }
    }
    //cout << seg.keys[l] << endl;
    // 复制it迭代器
    Iterator * it = my_sorted_view_.mydb->NewIterator(ReadOptions());
    it->Seek(seg.Cursor_Offsets[seg.Run_Selectors[l]]->key().ToString());
    current_ = it;
    current_anchor_key_ = left;
    current_segment_ = 0;
    for(size_t i = 0; i < seg.stept[l];){
      //cout << it->key().ToString() << " ";
        it->Next(my_sorted_view_,left,i);
        current_anchor_key_ = left;
        current_segment_ = i;
    }

  };
  int SeekToFirst() override{return 0;};
  void SeekToLast() override{};
  int Next() override{
    current_->Next(my_sorted_view_,current_anchor_key_,current_segment_);
    return 0;}
  void Prev() override{};
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
  void print(Segment seg,Iterator * current_){
    cout << "current_ key is " << current_->key().ToString() << endl;
    cout << "the keys in the Seg :" << endl;
    for(int i = 0; i < seg.size; i++){
      cout << seg.keys[i] << " ";
    }
    cout << endl;
    cout << "the stepts in the seg:" << endl;
    for(int i =0; i < seg.size; i++){
        cout << seg.stept[i] << " ";
    }
    cout << endl;
    cout << "the runs_sum in the seg:" << endl;
    for(int i =0; i < seg.runs_num; i++){
        cout << seg.runs_sum[i] << " ";
    }
    cout << endl;
    
  }
 size_t current_anchor_key_;
 size_t current_segment_;
 private:
  Iterator* current_;
  Remix my_sorted_view_;
  
};
} //namespace leveldb
