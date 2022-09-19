//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/index/b_plus_tree.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <string>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {
          LOG_DEBUG("init B+Tree ok;  leafMaxSize:%d, internalMaxSize:%d",leaf_max_size_,internal_max_size_);
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const { return root_page_id_ == INVALID_PAGE_ID; }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) {
  if (IsEmpty()){
    return false;
  }
  //查询，从根节点出发，直到找到叶子结点。每次查找都是二分。
  Page *leaf_page = FindLeafPage(key);
  LeafPage *leaf_node = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  ValueType value;
  bool ok = leaf_node->Lookup(key, &value, comparator_);

  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);//一页用完了就unpin掉，方便lru替换

  result->resize(1);
  (*result)[0] = value;
  if (!ok){    //树中无这个节点
    return false;
  }
  return true;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) {
  LOG_DEBUG("Insert");
  //伪代码在<数据库系统概论>P279
  //1，树为空，建立一个新节点作为根节点。
  if (IsEmpty()){
    LOG_DEBUG("TREE IS EMPTY");
    StartNewTree(key,value);
    return true;
  }
  //2,向非空B+Tree插入KV
  return InsertIntoLeaf(key,value);
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  LOG_DEBUG("StartNewTree");
  page_id_t pageId;
  Page *page = buffer_pool_manager_->NewPage(&pageId);
  if (page == nullptr){//新page创建失败
    throw ExceptionType::OUT_OF_MEMORY;
  }
  LeafPage *rootNode = reinterpret_cast<LeafPage*>(page->GetData());
  rootNode->Init(pageId,INVALID_PAGE_ID,leaf_max_size_);
  root_page_id_ = pageId;
  UpdateRootPageId();
  //插入KV
  rootNode->Insert(key,value,comparator_);
  buffer_pool_manager_->UnpinPage(pageId, true);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immdiately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) {
  //向B+Tree插入一个KV到叶子节点
  //1，找到待插入的leaf node；
  Page *leafPage = FindLeafPage(key);
  LeafPage *leafNode = reinterpret_cast<LeafPage *>(leafPage->GetData());

  //2,看看key是否重复
  ValueType searchValue;
  bool is_exit = leafNode->Lookup(key, &searchValue, comparator_);
  if (is_exit){//已存在的key，不添加
    LOG_DEBUG("Key has exited");
    buffer_pool_manager_->UnpinPage(leafPage->GetPageId(), false);//记住，凡是fetch/new的page，用完都要unpin
    return false;
  }
  LOG_DEBUG("Key not exit, begin insert");

  //插入key到leaf，如果满了就分裂。
  leafNode->Insert(key, value, comparator_);
  LOG_DEBUG("insert over,curSize:%d, maxSize:%d",leafNode->GetSize(),leafNode->GetMaxSize());

  if (leafNode->GetSize() >= leafNode->GetMaxSize()){
    LOG_DEBUG("size:%d ,> maxSize:%d,need split",leafNode->GetSize(),leaf_max_size_);
    LeafPage *newLeafNode = Split(leafNode);

    //把分裂出的新节点添加到当前节点的父节点上面，至于父节点的调整，也要在这个函数中完成。
    //拆分后将键值对插入父节点这个内部页面
    //在leaf节点分裂之后，新的node需要在父节点中插入一个Key+Pointer的pair对，用于指示newNode。
    //而在父节点插入的这个Key-Pointer的key，应当是newNode中的下限(newNode.keys>=key)，oldNode中的上限(oldNode.keys<key)
    //这个key就是newNode的第一个节点的key，array[0].first
    InsertIntoParent(leafNode,newLeafNode->KeyAt(0),newLeafNode);
  }

  buffer_pool_manager_->UnpinPage(leafPage->GetPageId(), true);
  return true;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
N *BPLUSTREE_TYPE::Split(N *node) {
  LOG_DEBUG("begain split");
  //TODO Split
  //新建一个节点，把一半KV从node节点迁移到新建的节点。这里N是泛型，这里指leafNode和internalNode
  page_id_t newPageId;
  Page *newPage = buffer_pool_manager_->NewPage(&newPageId);
  N *new_node = reinterpret_cast<N *>(newPage->GetData());
  //这里对leafPage和internalPage区分看待,因为两者的MoveHalfTo函数参数不同，不能使用一个泛型调用一个函数解决，需要强制类型转换
  if (node->IsLeafPage()){
    LOG_DEBUG("Splitting: leaf");
    new_node->Init(newPageId,node->GetParentPageId(),leaf_max_size_);
    LeafPage *oldNode = reinterpret_cast<LeafPage *>(node);
    LeafPage *newNode = reinterpret_cast<LeafPage *>(new_node);
    oldNode->MoveHalfTo(newNode);
    //设置叶子结点的前后连接指针,是一个单向链表
    newNode->SetNextPageId(oldNode->GetNextPageId());
    oldNode->SetNextPageId(newNode->GetPageId());
    LOG_DEBUG("SplitOver; LeafNode,minSize:%d, maxSize:%d, oldNodeSize:%d,newNodeSize:%d,",newNode->GetMinSize(),newNode->GetMaxSize(),oldNode->GetSize(),newNode->GetSize());
    //把oldNode的类型转换为N，保存为new_node返回
    new_node = reinterpret_cast<N *>(newNode);
  }else{//internal node
    LOG_DEBUG("Splitting: internal");
    new_node->Init(newPageId,node->GetParentPageId(),internal_max_size_);

    InternalPage *oldNode = reinterpret_cast<InternalPage *>(node);
    InternalPage *newNode = reinterpret_cast<InternalPage *>(new_node);
    oldNode->MoveHalfTo(newNode,buffer_pool_manager_);
    LOG_DEBUG("SplitOver; InternalNode,minSize:%d, maxSize:%d, oldNodeSize:%d,newNodeSize:%d,",newNode->GetMinSize(),newNode->GetMaxSize(),oldNode->GetSize(),newNode->GetSize());

    new_node = reinterpret_cast<N *>(newNode);
  }
  return new_node;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                                      Transaction *transaction) {
  LOG_DEBUG("InsertIntoParent after split");
  //在leaf节点分裂之后，新的node需要在父节点中插入一个Key+Pointer的pair对，用于指示newNode。
    //而在父节点插入的这个Key-Pointer的key，应当是newNode中的下限(newNode.keys>=key)，oldNode中的上限(oldNode.keys<key)
    //这个key就是newNode的第一个节点的key，array[0].first
  page_id_t parentPageId = old_node->GetParentPageId();

  //如果oldNode已经是root了，没有父节点，那么创建一个父节点，并且父节点指向俩子node;
  // 注意，此时新建的父节点就是root节点！
  if (old_node->IsRootPage()){
    LOG_DEBUG("split node is root,we need a new Root");
    page_id_t parentId;
    Page *parentPage = buffer_pool_manager_->NewPage(&parentId);
    if (parentPage == nullptr){
      throw ExceptionType::OUT_OF_MEMORY;
    }
    InternalPage *newRoot = reinterpret_cast<InternalPage *>(parentPage->GetData());
    newRoot->Init(parentId,INVALID_PAGE_ID,internal_max_size_);
    //我自己这个节点作为 旧root节点分裂后俩节点的父节点，而成为新的root节点；
    newRoot->PopulateNewRoot(old_node->GetPageId(),key,new_node->GetPageId());
    old_node->SetParentPageId(newRoot->GetPageId());
    new_node->SetParentPageId(newRoot->GetPageId());
    LOG_DEBUG("InsertIntoParent over, parent.curSize:%d, MaxSize:%d",newRoot->GetSize(),newRoot->GetMaxSize());

    //更新header page中的root page信息
    root_page_id_ = newRoot->GetPageId();
    UpdateRootPageId();

    //unpin使用过的页面，oldNode在调用该方法的函数中unpin
    buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(newRoot->GetPageId(),true);
    return;
  }

  LOG_DEBUG("split node is not root,just insert key into parent");

  //说明oldNode不是root，只是一个普通的节点，那么就在其父节点中搜索key的位置，然后插入即可！
  Page *pPage = buffer_pool_manager_->FetchPage(parentPageId);
  InternalPage *parentNode = reinterpret_cast<InternalPage *>(pPage->GetData());

  //在可能进入父节点递归分裂之前，先unpin已经无用的newNode
  new_node->SetParentPageId(parentPageId);
  page_id_t newNodeId = new_node->GetPageId();
  buffer_pool_manager_->UnpinPage(new_node->GetPageId(),true);

  //因为我们执行插入操作的时候，会拒绝已存在的key插入，所以根据本Tree的实现，只要是能执行到这里，必然key在tree中不存在过，而且不会在任何一个节点存在
  //不需要判断重复啦
  //因为是oldNode分裂出newNode，所以newNode在parent中的位置应该是oldNode.pageId之后。
  int size = parentNode->InsertNodeAfter(old_node->GetPageId(), key, newNodeId);
  LOG_DEBUG("internal node insert over,curSize:%d, maxSize:%d",parentNode->GetSize(),parentNode->GetMaxSize());

  if (size-1 >= parentNode->GetMaxSize()){//maxSize的意义是有效key的数量；size-1是有效key的数量
    //父节点已经超载，需要分裂！递归一直到不需要分裂的父节点！
    InternalPage *newPNode = Split(parentNode);
    InsertIntoParent(parentNode,newPNode->KeyAt(0),newPNode);
  }

  //释放父节点
  buffer_pool_manager_->UnpinPage(parentPageId,true);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {
  return false;
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::Coalesce(N **neighbor_node, N **node,
                              BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> **parent, int index,
                              Transaction *transaction) {
  return false;
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) { return false; }

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::begin() {
  //返回指向第一个leaf节点的第一个元素的迭代器
  //1，找到leftMost的 leafPage
  KeyType useless;
  Page *leftLeaf = FindLeafPage(useless, true);
  LeafPage *leafNode = reinterpret_cast<LeafPage *>(leftLeaf->GetData());
  return INDEXITERATOR_TYPE(buffer_pool_manager_, leafNode, 0);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) { 
  //先找到包含输入key的叶子页，搜索到key所在的index,再构造索引迭代器
  Page *p = FindLeafPage(key);
  LeafPage *leafNode = reinterpret_cast<LeafPage *>(p->GetData());
  ValueType value;
  int index = leafNode->KeyIndex(key,comparator_);
  return INDEXITERATOR_TYPE(buffer_pool_manager_,leafNode,index);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::end() {
//    构造一个索引迭代器，表示叶子节点中键值对的结束,结束的位置其实就是最右侧leafNode的最后一个元素之后。
    //首先找到第一个leafNode，然后搜索leafNode链表达到最后一个leafNode
    KeyType useless;
    Page *curPage = FindLeafPage(useless, true);
    LeafPage *curNode = reinterpret_cast<LeafPage *>(curPage->GetData());
    while(curNode->GetNextPageId()!=INVALID_PAGE_ID){
      buffer_pool_manager_->UnpinPage(curPage->GetPageId(), false);
      curPage = buffer_pool_manager_->FetchPage(curNode->GetNextPageId());
      curNode = reinterpret_cast<LeafPage *>(curPage->GetData());
    }
    //找到最右侧的leafPage
    return INDEXITERATOR_TYPE(buffer_pool_manager_,curNode,curNode->GetSize()-1);
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 * leftMost节点为true时，代表我们要找到leafPage是最左侧的一个leafPage，也就是第一个leafPage
 */
INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool leftMost) {
  //寻找包含Key的leaf节点。
  Page *cur_page = buffer_pool_manager_->FetchPage(root_page_id_);
  BPlusTreePage *cur_node = reinterpret_cast<BPlusTreePage *>(cur_page->GetData());//表示目前正在查找的节点，从rootPage开始；
                                                                                   // 注意，treePage是page存储的data！
  while(!cur_node->IsLeafPage()){
    InternalPage *node = reinterpret_cast<InternalPage *>(cur_node);
    page_id_t child_node_id;
    if (leftMost){
      //如果要寻找最左侧leaf，那么直接搜索internalNode的第一个元素的ptr就行
      child_node_id = node->ValueAt(0);
    }else{
      child_node_id = node->Lookup(key, comparator_);
    }
    buffer_pool_manager_->UnpinPage(cur_page->GetPageId(), false);

    cur_page = buffer_pool_manager_->FetchPage(child_node_id);
    cur_node = reinterpret_cast<BPlusTreePage *>(cur_page->GetData());
  }

  //到这里说明已经到达了B+Tree的叶子节点，直接搜索页子节点就行。
  return cur_page;
  //  throw Exception(ExceptionType::NOT_IMPLEMENTED, "Implement this for test");

}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  HeaderPage *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't  need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    InternalPage *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    InternalPage *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
