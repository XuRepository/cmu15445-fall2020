//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "common/logger.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
    SetPageId(page_id);
    SetParentPageId(parent_id);
    SetMaxSize(max_size);
    SetSize(0);
    SetPageType(IndexPageType::INTERNAL_PAGE);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const {
  // replace with your own code
  assert(index >= 0 && index < GetSize());
  return array[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  assert(index >= 0 && index < GetSize());
  array[index].first = key;
}

/*
 * 寻找对应value的下标
 * Helper method to find and return array index(or offset), so that its value
 * equals to input "value"
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const {
  // 对于内部页面，key有序可以比较，但value无法比较，只能顺序查找;
  //因为是查找value，所以要从0，header开始查找，因为header的value指向最左侧的下一层page
  //对于internalPage，只是第一个key无效，第一个value还是有效的。
  for (int i = 0; i < GetSize(); ++i) {
    if(array[i].second == value){
      return i;
    }
  }
  return -1;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const {
  LOG_DEBUG("index:%d,curSize:%d,maxSize:%d",index,GetSize(),GetMaxSize());
  assert(index >= 0 && index < GetSize());
  return array[index].second ;
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key, const KeyComparator &comparator) const {
  //寻找给定的key在array中符合 K(i)<=key<K(i+1)的i，这个pair的value值就是key所在的internal/leaf page
  //采用二分查找，
//  assert(GetSize()>1);
  int left = 1;
  int right = GetSize()-1;
  while (left <= right){
    int mid = (left+right)/2;
    if (comparator(KeyAt(mid),key) >0){
      //mid大，目标在左侧区间
      right = mid -1;
    } else{
      //注意，即使找到了，也令left右移；这样可以保证最终的查找结果是left位于比key大的一个数的下标，所以实际上寻找的key是left-1
      left = mid +1;
    }
  }
  int keyIndex = left -1 ;
  assert(comparator(KeyAt(keyIndex),key)<=0);
  return ValueAt(keyIndex);
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(const ValueType &old_value, const KeyType &new_key,
                                                     const ValueType &new_value) {
  //实际上就是分裂root节点,我自己这个节点作为旧root节点分裂后俩节点的父节点，而成为新的root节点；
    array[0].second = old_value;
    array[1].second = new_value;
    array[1].first = new_key;
    SetSize(2);
}
/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * 在value=oldValue的pair对之后插入新的一对，一般用于子节点分裂之后，newNode插入到parent中。
 * 而分裂后的newKV应该紧跟着oldKV
 * @return:  new size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(const ValueType &old_value, const KeyType &new_key,
                                                    const ValueType &new_value) {
  int insertIndex = ValueIndex(old_value)+1;
  assert(insertIndex>0);
  for (int i = GetSize(); i > insertIndex ; --i) {
    array[i] = array[i-1];
  }
  array[insertIndex] = MappingType{new_key,new_value};
  IncreaseSize(1);
  return GetSize();
}

/*****************************************************************************
 * SPLIT
 * * 上层调用：
* this page是old_node，recipient page是new_node
* old_node的右半部分array复制给new_node
* 并且，将new_node（原old_node的右半部分）的所有孩子结点的父指针更新为指向new_node
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 *  * 参数中需要bufferPool是因为internal节点有子页，需要把子页从磁盘拉进来修改parent。

 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage *recipient,
                                                BufferPoolManager *buffer_pool_manager) {
  //用于节点分裂的时候，向新节点copy数据
  //内部节点第一个pair无效，所以复制到recipient的有效key实际上是this->array[getMinSize(),end);
  //但是，考虑到B+Tree分裂后的新节点的第一个pair同样是无效key，而且我们需要直到第一个pair的value（指针）的范围，也需要使用这个key来给node的父节点填充，指示（key，pointer）对。
  //所以说我们需要在分裂之后，知道新node中key的最小值，也就是newNode的第一个无效key！
  //oldNode:  k0 p0 | ...| kn pn;
  //newNode:  kn+1 pn+1|....   注意实际上kn+1是位于第一个pair，无效。但是它可以指示新的node里面的key>= kn+1;
  // 而这个kn+1也会被用于插入到到newNode的父节点，pointer是newNode自己。
  //而在父节点插入的这个Key-Pointer的key，应当是newNode中的下限(newNode.keys>=key)，oldNode中的上限(oldNode.keys<key)
    int startIndex = GetMinSize();
    int copy_num = GetSize()-startIndex;
    recipient->CopyNFrom(array+startIndex,copy_num,buffer_pool_manager);
    IncreaseSize(-copy_num);
}

/* Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyNFrom(MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {
  //将[items,items+size)复制到当前page的array最后一个之后的空间
  int oldSize = GetSize();//拷贝之前我自己的size
  std::copy(items,items+size,array+GetSize());
  IncreaseSize(size);
  for (int i = oldSize; i < GetSize(); ++i) {
    //首先根据pageId，从磁盘中拉取子树节点的page，然后修改子page的parent为自己，原本page是分裂之前的page；
    int child_page_id = ValueAt(i);
    Page *child_page = buffer_pool_manager->FetchPage(child_page_id);
    //拉取到的page强制转换为B+树page
    BPlusTreePage *child = reinterpret_cast<BPlusTreePage*>(child_page);

    //修改父节点id，并且unpin等待写回。
    child->SetParentPageId(this->GetPageId());
    buffer_pool_manager->UnpinPage(child_page_id, true);
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {
  for (int i = index; i < GetSize(); ++i) {
    array[i] = array[i+1];
  }
  IncreaseSize(-1);
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() {
  ValueType value = ValueAt(0);
  SetSize(0);
  return value;
}
/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                               BufferPoolManager *buffer_pool_manager) {
  //TODO 这个方法做啥捏

  // 当前node的第一个key(即array[0].first)本是无效值(因为是内部结点)，但由于要移动当前node的整个array到recipient
  // 那么必须在移动前将当前node的第一个key 赋值为 父结点中下标为index的middle_key
  SetKeyAt(0, middle_key);  // 将分隔key设置在0的位置
  recipient->CopyNFrom(array, GetSize(), buffer_pool_manager);
  // 对于内部结点的合并操作，要把需要删除的内部结点的叶子结点转移过去
  // recipient->SetKeyAt(GetSize(), middle_key);
  SetSize(0);

}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient" page.
 *
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                      BufferPoolManager *buffer_pool_manager) {}

/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {}

/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                       BufferPoolManager *buffer_pool_manager) {}

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
