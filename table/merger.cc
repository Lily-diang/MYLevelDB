// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/merger.h"

#include "leveldb/comparator.h"
#include "leveldb/iterator.h"
#include "table/iterator_wrapper.h"
#include "leveldb/RemixHelper.h"
#include "leveldb/Remix.h"

namespace leveldb {

namespace {

class MergingIterator : public Iterator, public Remix_Helper {
 public:
  MergingIterator(const Comparator* comparator, Iterator** children, int n)
      : comparator_(comparator),
        children_(new IteratorWrapper[n]),
        n_(n),
        current_(nullptr),
        direction_(kForward) {
    for (int i = 0; i < n; i++) {
      children_[i].Set(children[i]);
    }
  }

  ~MergingIterator() override { delete[] children_; }

  bool Valid() const override { return (current_ != nullptr); }

  int SeekToFirst() override {
    for (int i = 0; i < n_; i++) {
      children_[i].SeekToFirst();
    }
    set_run_index(FindSmallest());
    direction_ = kForward;
    return get_run_index();
  }

  void SeekToLast() override {
    for (int i = 0; i < n_; i++) {
      children_[i].SeekToLast();
    }
    FindLargest();
    direction_ = kReverse;
  }
  // 注意调用后方向一定为kForward，也即从小到大
  void Seek(const Slice& target) override {
    for (int i = 0; i < n_; i++) {
      children_[i].Seek(target);
    }
    FindSmallest();
    direction_ = kForward;
  }
  // 注意：next（）永远找的>key（）的下一个位置，调用后方向一定为kForward
  int Next() override {
    assert(Valid());

    // Ensure that all children are positioned after key().
    // If we are moving in the forward direction, it is already
    // true for all of the non-current_ children since current_ is
    // the smallest child and key() == current_->key().  Otherwise,
    // we explicitly position the non-current_ children.
    // next()是只能前向移动，也就是找到一个比key()大的key()
    // 那么，如果移动方向不是前向的，就需要seek(key())
    // 然后再移动到刚好比key()大的地方
    if (direction_ != kForward) {
      for (int i = 0; i < n_; i++) {
        IteratorWrapper* child = &children_[i];
        // current_不需要移动，因为这个key（）指的就是current_的key
        if (child != current_) {
          // 那么把这个iterator移动>= key()的地方
          child->Seek(key());
          if (child->Valid() &&
              comparator_->Compare(key(), child->key()) == 0) {
            child->Next();
          }
        }
      }
      direction_ = kForward;
    }

    current_->Next();
    set_run_index(FindSmallest());
    return get_run_index();
  }

  //##################
  void Next(Remix *my_sorted_view, size_t &index_anchor_key, size_t &segment_index)  override{
    assert(Valid());

    current_->Next();
    int index = FindSmallest(my_sorted_view,index_anchor_key,segment_index);
    if(index < 0){
      current_ = nullptr;
    }
    else{
      current_ = &children_[index];
    }
  }
  // 注意：next（）永远找的都是<key（）的下一个位置,逻辑与next()类似，且调用了Prev()后，方向一定为kReverse
  void Prev() override {
    assert(Valid());

    // Ensure that all children are positioned before key().
    // If we are moving in the reverse direction, it is already
    // true for all of the non-current_ children since current_ is
    // the largest child and key() == current_->key().  Otherwise,
    // we explicitly position the non-current_ children.
    // Prev（）为反向移动，所以默认方向为kReverse
    if (direction_ != kReverse) {
      for (int i = 0; i < n_; i++) {
        IteratorWrapper* child = &children_[i];
        // 对于非current_，首先要找到>=key()的位置
        if (child != current_) {
          child->Seek(key());
          if (child->Valid()) {
            // 由于child>=key()，因此只需要Prev()就能找到<key()的位置
            // Child is at first entry >= key().  Step back one to be < key()
            child->Prev();
          } else {
            // Child has no entries >= key().  Position at last entry.
            // 如果整个迭代器都小于key()，那么就返回迭代器末尾元素
            child->SeekToLast();
          }
        }
      }
      direction_ = kReverse;
    }
    // 对current_进行prev操作
    current_->Prev();
    // 更改当前指针为最大的元素
    FindLargest();
  }

  Slice key() const override {
    assert(Valid());
    return current_->key();
  }

  Slice value() const override {
    assert(Valid());
    return current_->value();
  }

  Status status() const override {
    Status status;
    for (int i = 0; i < n_; i++) {
      status = children_[i].status();
      if (!status.ok()) {
        break;
      }
    }
    return status;
  }

  void set_run_index(int index) { // !!!
    assert(index>=0);
    run_index = index;
  }

  int get_run_index() {  // !!!
    return run_index;
  }

 private:
  // Which direction is the iterator moving?
  enum Direction { kForward, kReverse };

  int FindSmallest();
  int FindSmallest(Remix *my_sorted_view, size_t &index_anchor_key, size_t &segment_index); // ##########
  void FindLargest();

  // We might want to use a heap in case there are lots of children.
  // For now we use a simple array since we expect a very small number
  // of children in leveldb.
  // 我们可能想使用一个堆，以防有很多children。现在我们使用一个简单的数组，因为我们期望小数目的children在leveldb中
  const Comparator* comparator_;
  IteratorWrapper* children_;  // 指向children数组的指针
  int n_;
  IteratorWrapper* current_;
  Direction direction_;
  int run_index; // !!!
};


/**
 * @brief 遍历整个children_迭代器，找出最小的一个节点，设置为current_
 * @return {*}
 */
int MergingIterator::FindSmallest() {
  IteratorWrapper* smallest = nullptr;
  int index = 0;
  for (int i = 0; i < n_; i++) {
    IteratorWrapper* child = &children_[i];
    if (child->Valid()) {
      if (smallest == nullptr) {
        smallest = child;
        index = i;
      } else if (comparator_->Compare(child->key(), smallest->key()) < 0) {
        smallest = child;
        index = i;
      }
    }
  }
  current_ = smallest;
  return index;
}

// ##############
int MergingIterator::FindSmallest(Remix *my_sorted_view, size_t &index_anchor_key, size_t &segment_index) {
  //IteratorWrapper* smallest = nullptr;
  Segment seg = my_sorted_view->segments[index_anchor_key];
  int index;
  if(segment_index+1 < seg.size){
    index = seg.Run_Selectors[segment_index+1];
    segment_index++;
  }
  else if(index_anchor_key + 1 < my_sorted_view->segment_size){
    index = my_sorted_view->segments[index_anchor_key+1].Run_Selectors[0];
    index_anchor_key++;
  }
  else{
    index = -1;
  }
  
  //current_ = smallest;
  return index;
}

/**
 * @brief 遍历整个children_迭代器，找出最大的一个节点 ，设置为current_
 * @return {*}
 */
void MergingIterator::FindLargest() {
  IteratorWrapper* largest = nullptr;
  for (int i = n_ - 1; i >= 0; i--) {
    IteratorWrapper* child = &children_[i];
    if (child->Valid()) {
      if (largest == nullptr) {
        largest = child;
      } else if (comparator_->Compare(child->key(), largest->key()) > 0) {
        largest = child;
      }
    }
  }
  current_ = largest;
}
}  // namespace


// leveldb就是在此处产生的全局有序视图！！！
Iterator* NewMergingIterator(const Comparator* comparator, Iterator** children,
                             int n) {
  assert(n >= 0);
  if (n == 0) {
    return NewEmptyIterator();
  } else if (n == 1) {
    return children[0];
  } else { //当输入超过1个Iterator时，会生成一个Merger Iterator
    return new MergingIterator(comparator, children, n);
  }
}

}  // namespace leveldb
