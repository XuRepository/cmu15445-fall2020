//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/b_plus_tree_page.h"

namespace bustub {

/*
 * Helper methods to get/set page type
 * Page type enum class is defined in b_plus_tree_page.h
 */
bool BPlusTreePage::IsLeafPage() const {
  return page_type_==IndexPageType::LEAF_PAGE;
}
bool BPlusTreePage::IsRootPage() const {
  //root节点没有父节点，其他节点都存在父节点。
  return parent_page_id_ == INVALID_PAGE_ID;
}
void BPlusTreePage::SetPageType(IndexPageType page_type) {page_type_ = page_type;}

/*
 * Helper methods to get/set size (number of key/value pairs stored in that
 * page)
 */
int BPlusTreePage::GetSize() const { return size_; }
void BPlusTreePage::SetSize(int size) {
  size_ = size;
}
void BPlusTreePage::IncreaseSize(int amount) {
  size_+=amount;
}

/*
 * Helper methods to get/set max size (capacity) of the page
 */
int BPlusTreePage::GetMaxSize() const { return max_size_; }
void BPlusTreePage::SetMaxSize(int size) {max_size_ = size;}

/*
 * Helper method to get min page size
 * Generally, min page size == max page size / 2
 * 每个page所应当拥有的最小pair对数量！
 *
 * 根节点很特殊，因为根节点可以拥有少于半数的元素，同时根节点既可能是内部节点，也可能是叶节点；
 * 内部节点和叶节点也存在很大的区别：内部节点的第一个数组项的键值对中的键是不使用的，而叶节点则是全部使用的。
 * 对于rootPage，
 * 如果是叶子节点，最少要有1个pair；
 * 如果是内部节点，则至少需要使用两个pair对！因为对于内部节点而言，有一个pair的key是无效的。而作为一个树节点，怎么说也得有一个key才行。
 *
 * 对于非根节点，也要分开看，因为internal有无效节点，而max的含义是每个节点中有效key的个数
 * 对于leaf，//leaf节点 向下取整,(max_size_)/2；如此一来在size>=maxSize（节点Split的条件）的情况下，无论maxSize奇偶，从minSize开始分裂可以和保证分裂的俩节点都有>=minSize的KV数量。

 //internal节点 向上取整；如此一来在size-1>=maxSize（节点Split的条件）的情况下，无论maxSize是奇数偶数，都可以保证俩节点有>=minSize的KV数
//例如，如果maxSize=8，那么curSize=9的时候，从minSize+1开始，分裂为4个和5个
//如果maxsize = 7，那么7/2向上取整为4，minsize=4，当curSize=8的时候分裂为4个和4个
//以上计算对于leaf节点没有问题，因为leaf节点没有无效key
//但是对于internal节点，我们认为可一个无效key的pair也算在curSize之内，因此每次分裂出的新节点，也可以确保在无效pair+有效pair的大小仍然在>=minSize
//eg： 对于internal节点如果maxsize = 7，那么7/2向上取整为4，minsize=4，当curSize=8的时候分裂为4个和4个，分裂出的俩节点第一个均为无效key，但是大小满足大于等于minSize

 */
int BPlusTreePage::GetMinSize() const {
  //每个page所应当拥有的最小pair对数量！
  //maxSize的含义是每个节点应当拥有的有效pair对数量。
//  return max_size_/2;
  if (IsRootPage()){
    return IsLeafPage()?1:2;
  }
    if (IsLeafPage()){
      return (max_size_)/2;//
    }else{
      return (max_size_)/2;//internal节点 ，向上取整可以保证每个节点在奇数偶数的情况下都可以有>=minSize的大小
    }
//    return max_size_/2;
}

/*
 * Helper methods to get/set parent page id
 */
page_id_t BPlusTreePage::GetParentPageId() const { return parent_page_id_; }
void BPlusTreePage::SetParentPageId(page_id_t parent_page_id) {parent_page_id_ = parent_page_id;}

/*
 * Helper methods to get/set self page id
 */
page_id_t BPlusTreePage::GetPageId() const { return page_id_; }
void BPlusTreePage::SetPageId(page_id_t page_id) {
  page_id_ = page_id;
}

/*
 * Helper methods to set lsn
 */
void BPlusTreePage::SetLSN(lsn_t lsn) { lsn_ = lsn; }
}  // namespace bustub
