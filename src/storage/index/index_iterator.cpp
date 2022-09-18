/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
//INDEX_TEMPLATE_ARGUMENTS
//INDEXITERATOR_TYPE::IndexIterator() = default;

////泛型方法
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *b, IndexIterator::LeafPage *leafNode,
                                                                int index) {
  bufferPoolManager = b;
  this->leafNode = leafNode;
  this->curIndex = index;
}
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator(){
  if (leafNode!= nullptr){
    bufferPoolManager->UnpinPage(leafNode->GetPageId(),false);
  }
};

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::isEnd() {
  if (leafNode == nullptr || curIndex >= leafNode->GetSize()){
    //这里我们规定，如果遍历过程中发现curIndex>=getSize,就拉取下一页更新leafNode，并且更新curIndex=0
    //如果发现没有下一页，更新leafNode为nullptr。
    return true;
  }
//  if (curIndex == leafNode->GetSize()-1){//已经到了arrays数组的最后一个元素
//    if (leafNode->GetNextPageId() == INVALID_PAGE_ID){//已经是最后一个leafNode
//      return true;
//    }
//  }
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
const MappingType &INDEXITERATOR_TYPE::operator*() {
  //相当于获取当前iter所指向的元素
    return leafNode->GetItem(curIndex);
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE &INDEXITERATOR_TYPE::operator++() {
  //相当于 iter.next();
  curIndex++;
  if (leafNode!= nullptr && curIndex >= leafNode->GetSize()){
    //这一页最后一个元素就是getSize-1对应下标的元素，所以到这里说明这一页已经遍历结束
    if (leafNode->GetNextPageId() == INVALID_PAGE_ID){//最后一页
      bufferPoolManager->UnpinPage(leafNode->GetPageId(), false);
      this->leafNode = nullptr;
    }else{
      //拉去新的leafPage
      Page *p = bufferPoolManager->FetchPage(leafNode->GetNextPageId());
      bufferPoolManager->UnpinPage(leafNode->GetPageId(), false);
      leafNode = reinterpret_cast<LeafPage *>(p->GetData());
      curIndex = 0;
    }
  }
  return *this;
}
//template <typename KeyType, typename ValueType, typename KeyComparator>


template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
