//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.h
//
// Identification: src/include/buffer/lru_replacer.h
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <list>
#include <mutex>  // NOLINT
#include <vector>

#include "buffer/replacer.h"
#include "common/config.h"
#include "unordered_map"

namespace bustub {

/**
 * LRUReplacer implements the lru replacement policy, which approximates the Least Recently Used policy.
 */
class LRUReplacer : public Replacer {
  //.h文件用来声明，方法的实现是放在.cpp文件中！
 public:
  /**
   * Create a new LRUReplacer.
   * @param num_pages the maximum number of pages the LRUReplacer will be required to store
   */
  // explicit关键字的作用就是防止类构造函数的隐式自动转换
  //explicit关键字只能用来修饰类内部的构造函数声明，作用于单个参数的构造函数；被修饰的构造函数的类，不能发生相应的隐式类型转换。
  explicit LRUReplacer(size_t num_pages);

  /**
   * Destroys the LRUReplacer.
   */
  ~LRUReplacer() override;

  bool Victim(frame_id_t *frame_id) override;

  void Pin(frame_id_t frame_id) override;

  void Unpin(frame_id_t frame_id) override;

  size_t Size() override;

 private:
  // TODO(student): implement me!
  std::mutex latch_;
  size_t max_size;
  //这里我们用了链表 + hash表。主要是为了删除和插入均为0(1)的时间复杂度。
  //引入hash表就是可以根据frame_id快速找到其在list中对应的位置。否则的话你需要遍历链表这就不是o(1)了

  //维护lurReplacer中所存放的frame集合，frame是BufferPool的，page是磁盘的；
  //这里我们认为，链表的尾部是最久未访问，链表的头部是最近访问。
  std::list<frame_id_t> lru_list;

  //注意，map的v是一个迭代器，指向的应该是当前frame_id在lru_list中的位置
  std::unordered_map<frame_id_t,std::list<frame_id_t>::iterator> lru_map;
};

}  // namespace bustub
