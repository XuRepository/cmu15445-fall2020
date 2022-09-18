//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 * 迭代器的功能就是顺序遍历一颗B+Tree的叶子节点，因为叶子节点是单向数组链表。
 */
#pragma once
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
  using LeafPage = B_PLUS_TREE_LEAF_PAGE_TYPE;
 public:
  // you may define your own constructor based on your member variables
//  IndexIterator();
  IndexIterator(BufferPoolManager *b,LeafPage *leafNode,int index);
  ~IndexIterator();

  bool isEnd();

  const MappingType &operator*();

  IndexIterator &operator++();

  bool operator==(const IndexIterator &itr) const { throw std::runtime_error("unimplemented"); }

  bool operator!=(const IndexIterator &itr) const { throw std::runtime_error("unimplemented"); }

 private:
  // add your own private member variables here
  LeafPage *leafNode; //当前正在被iter遍历的leafPage。
  int curIndex; //当前iter指向的pair，在当前leafPage的下标。

  //因为B+Tree的leafNode是一个leafNode的链表，
  // 所以需要在当前leafPage遍历结束之后，通过bufferPool拉取下一页
  BufferPoolManager *bufferPoolManager;
};

}  // namespace bustub
