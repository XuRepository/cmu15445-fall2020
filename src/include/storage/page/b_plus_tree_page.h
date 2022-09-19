//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/page/b_plus_tree_page.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include <cassert>
#include <climits>
#include <cstdlib>
#include <string>

#include "buffer/buffer_pool_manager.h"
#include "storage/index/generic_key.h"

namespace bustub {

#define MappingType std::pair<KeyType, ValueType>

#define INDEX_TEMPLATE_ARGUMENTS template <typename KeyType, typename ValueType, typename KeyComparator>

// define page type enum
enum class IndexPageType { INVALID_INDEX_PAGE = 0, LEAF_PAGE, INTERNAL_PAGE };

/**
 * Both internal and leaf page are inherited from this page.
 *
 * It actually serves as a header part for each B+ tree page and
 * contains information shared by both leaf page and internal page.
 *
 * Header format (size in byte, 24 bytes in total):
 * ----------------------------------------------------------------------------
 * | PageType (4) | LSN (4) | CurrentSize (4) | MaxSize (4) |
 * ----------------------------------------------------------------------------
 * | ParentPageId (4) | PageId(4) |
 * ----------------------------------------------------------------------------
 */
class BPlusTreePage {
 public:
  bool IsLeafPage() const;
  bool IsRootPage() const;
  void SetPageType(IndexPageType page_type);

  int GetSize() const;
  void SetSize(int size);
  void IncreaseSize(int amount);

  int GetMaxSize() const;
  void SetMaxSize(int max_size);
  int GetMinSize() const;

  page_id_t GetParentPageId() const;
  void SetParentPageId(page_id_t parent_page_id);

  page_id_t GetPageId() const;
  void SetPageId(page_id_t page_id);

  void SetLSN(lsn_t lsn = INVALID_LSN);

 private:
  // member variable, attributes that both internal and leaf page share

  //使用 __attribute__ ((packed)) ，让编译器取消结构在编译过程中的优化对齐,
  //按照实际占用字节数进行对齐，这样子两边都需要使用 __attribute__ ((packed))取消优化对齐，就不会出现对齐的错位现象。

  IndexPageType page_type_ __attribute__((__unused__));
  lsn_t lsn_ __attribute__((__unused__));
  int size_ __attribute__((__unused__));
  //最大有效key的数量，所以每个节点的最少key数量为（maxSize+1）/2，+1为了实现向上取整。
  //如此一来，如果maxSize=8，那么curSize=9的时候，从minSize+1开始，分裂为4个和5个
  //如果maxsize = 7，那么7/2向上取整为4，minsize=4，当curSize=8的时候分裂为4个和4个
  //以上计算对于leaf节点没有问题，因为leaf节点没有无效key
  //但是对于internal节点，我们认为可一个无效key的pair也算在curSize之内，因此每次分裂出的新节点，也可以确保在无效pair+有效pair的大小仍然在>=minSize
  //eg： 对于internal节点如果maxsize = 7，那么7/2向上取整为4，minsize=4，当curSize=8的时候分裂为4个和4个，分裂出的俩节点第一个均为无效key，但是大小满足大于等于minSize
  int max_size_ __attribute__((__unused__));
  page_id_t parent_page_id_ __attribute__((__unused__));
  page_id_t page_id_ __attribute__((__unused__));
};

}  // namespace bustub
