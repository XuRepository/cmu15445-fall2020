//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"
#include "common/logger.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) {
  LOG_DEBUG("asasasasas");
  max_size = num_pages;
}

LRUReplacer::~LRUReplacer() = default;

/**
 * 使用LRU策略删除一个victim frame，这个函数能得到frame_id
 * @param frame_id
 * @return
 */
bool LRUReplacer::Victim(frame_id_t *frame_id) {
  // C++17 std::scoped_lock
  // 它能够避免死锁发生，其构造函数能够自动进行上锁操作，析构函数会对互斥量进行解锁操作，保证线程安全。
  std::scoped_lock lock{latch_};

  if (lru_list.empty()){
    return false;
  }
  //移除并保存链表的第一个,链表的尾部是最久未访问，链表的头部是最近访问。
  *frame_id = lru_list.back();

  //删除frame_id这个frame
  lru_map.erase(*frame_id);
  lru_list.pop_back();

  return true;
}

/**
 * 固定一个frame, 表明它不应该成为victim（即在replacer中移除该frame_id）
 * pinned ：存放了 thread 正在使用的 page
    unpinned ：存放了 page，但 page 已经不再为任何 thread 所使用
 * @param frame_id
 */
void LRUReplacer::Pin(frame_id_t frame_id) {
  // C++17 std::scoped_lock
  // 它能够避免死锁发生，其构造函数能够自动进行上锁操作，析构函数会对互斥量进行解锁操作，保证线程安全。
  std::scoped_lock lock{latch_};

  //我的map中未维护该frame id
  if (lru_map.count(frame_id) == 0){
    return;
  }
  auto iter = lru_map[frame_id];
  lru_list.erase(iter);
  lru_map.erase(frame_id);

}

/**
 *  * 取消固定一个frame, 表明它可以成为victim（即将该frame_id添加到replacer）
 * @param frame_id
 */
void LRUReplacer::Unpin(frame_id_t frame_id) {
  // C++17 std::scoped_lock
  // 它能够避免死锁发生，其构造函数能够自动进行上锁操作，析构函数会对互斥量进行解锁操作，保证线程安全。
  std::scoped_lock lock{latch_};
  //链表的尾部是最久未访问，链表的头部是最近访问。所以添加unpin的frame的时候，直接加到链表头部；
  if (lru_map.count(frame_id) != 0){
    //已经被添加进来了，不用管
    return;
  }
  if (lru_list.size() == max_size){
    //已经达到最大容量了。
    return;
  }
  lru_list.push_front(frame_id);
  //获取在list中的迭代器的位置，加入到map
  lru_map.emplace(frame_id,lru_list.begin());
}

size_t LRUReplacer::Size() { return lru_list.size(); }

}  // namespace bustub
