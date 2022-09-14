//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include <list>
#include <unordered_map>

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}

/**
 * 该函数实现了缓冲池的主要功能：向上层提供指定的 page
 * @param page_id
 * @return
 */
Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  std::scoped_lock lock{latch_};
  Page *page;
  frame_id_t frame_id;
      
  auto iter = page_table_.find(page_id);
  //1.1 如果page table中存在这个table,也就是需要的pageId在pool中已经，可能在pined或者replacer中；
  if (iter!=page_table_.end()){
    // std::map<X, Y>实际储存了一串std::pair<const X, Y>
    // 这里，如果你用*it，那么你将得到map第一个元素的std::pair：
    // 现在你可以接收std::pair的两个元素：
    //(*it).first会得到key，
    //(*it).second会得到value。
    frame_id = iter->second;
    page = &pages_[frame_id];//iter->second是frame id
    if (page->pin_count_++ == 0){
      //pin count=0的page是在replacer中维护
      replacer_->Pin(frame_id);
    }
    return page;
  }
  //1.2 page在pool中不存在，需要引入page从磁盘
  //如果freeList还有空闲frame，就去一个freeFrame来保存目标page；
  //如果freeList已经空了。那就要从replacer中替换掉一页，用来加载目标page；
  frame_id = GetVictimFrameId();
  if (frame_id == INVALID_PAGE_ID){
    return nullptr;
  }
  page = &pages_[frame_id];
  // 2.     If Page is dirty, write it back to the disk.
  if (page->IsDirty()){
    disk_manager_->WritePage(page->page_id_,page->data_);
  }

  // 3.     Delete Page from the page table and insert P.
  page_table_.erase(page->page_id_);//这里的pageId为被替换出去的page的id
  page_table_[page_id] = frame_id;//这里的page_id是我们需要引入的目标page

  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  disk_manager_->ReadPage(page_id,page->data_);

  page->page_id_ = page_id;
  page->pin_count_ = 1;
  page->is_dirty_ = false;

  replacer_->Pin(frame_id);

  return page;
}

/**
 * 获取一个可以用于被加载的页，可以使空闲页，也可以是replacer页;
 * @return 如果找不到合适的页面，返回INVALID_PAGE_ID
 */
frame_id_t BufferPoolManager::GetVictimFrameId(){
  frame_id_t frame_id = INVALID_PAGE_ID;
  if (!free_list_.empty()){
    frame_id = free_list_.front();
    free_list_.pop_front();
  }else{
    //空闲页没有了，需要替换replacer
    bool ok = replacer_->Victim(&frame_id);
    if (!ok){
      //replacer为空
      return frame_id;
    }
  }
  return frame_id;
}

/**
 * 该函数用以减少对某个页的引用数 pin count，当 pin_count 为 0 时需要将其添加到 Replacer 中：
 * @param page_id
 * @param is_dirty
 * @return
 */
bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
  std::scoped_lock lock{latch_};

  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end()){
    //缓冲池中没有这个page
    return false;
  }

  frame_id_t frameId = iter->second;
  Page *page = &pages_[frameId];
  if (page->pin_count_<=0){
    return false;
  }

  page->is_dirty_ |= is_dirty;//更新dirty状态
  page->pin_count_--;
  if (page->pin_count_ == 0){
    replacer_->Unpin(frameId);
  }
  return true;
}

/**
 * 如果缓冲池的 page 被修改过，需要将其写入磁盘以保持同步：
 * @param page_id
 * @return
 */
bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  //从page table寻找目标page，把dirty页写入disk
  std::scoped_lock lock{latch_};

  auto iter = page_table_.find(page_id);
  if (iter==page_table_.end()){
    return false;
  }

  frame_id_t frameId = iter->second;
  Page *page = &pages_[frameId];
  if (page->is_dirty_){
    disk_manager_->WritePage(page->page_id_,page->data_);
    page->is_dirty_ = false;
  }
  return true;
}

/**
 * 该函数在缓冲池中插入一个新页，如果缓冲池中的所有页面都正在被线程访问，插入失败，否则靠 GetVictimFrameId() 计算插入位置：
 * @param page_id
 * @param is_dirty
 * @return
 */
Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  std::scoped_lock lock{latch_};

  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  if (free_list_.empty() && replacer_->Size() == 0){
    *page_id = INVALID_PAGE_ID;
    return nullptr;
  }
  
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  frame_id_t frameId = GetVictimFrameId();
  if (frameId == INVALID_PAGE_ID){
    *page_id = INVALID_PAGE_ID;
    return nullptr;
  }
  Page *page = &pages_[frameId];
  if (page->is_dirty_){
    disk_manager_->WritePage(page->page_id_,page->data_);
  }

  // 3.   Update P's metadata, zero out memory and add P to the page table.
  //Make sure you call DiskManager::AllocatePage!
  *page_id = disk_manager_->AllocatePage();//分配在磁盘一个新的page，返回id
  page_table_.erase(page->page_id_);
  page_table_[*page_id] = frameId;

  page->page_id_ = *page_id;
  page->pin_count_ = 1;//注意新建页面的pin为1
  page->is_dirty_ = false;
  page->ResetMemory();//新建的空页面，data要重置。

  replacer_->Pin(frameId);
  return page;
}

/**
 * 该函数从缓冲池和数据库文件中删除一个 page，并将其 page_id 设置为 INVALID_PAGE_ID
 * @param page_id
 * @return
 */
bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  std::scoped_lock lock{latch_};

  auto iter = page_table_.find(page_id);

  // 1.   If P does not exist, return true.
  if (iter == page_table_.end()){
    //缓冲池不存在该页
    return true;
  }

  frame_id_t frameId = iter->second;
  Page *page = &pages_[frameId];
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  if (page->pin_count_>0){
    return false;
  }

  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  disk_manager_->DeallocatePage(page_id);
  replacer_->Pin(frameId);//目的是从replacer中移除这个page
  page_table_.erase(page_id);

  page->page_id_ = INVALID_PAGE_ID;
  page->pin_count_ = 0;
  page->is_dirty_ = false;
  page->ResetMemory();

  free_list_.push_back(frameId);
  return true;
}

/**
 * 该函数将缓冲池中的所有 page 写入磁盘：
*/
void BufferPoolManager::FlushAllPagesImpl() {
  // You can do it!
  for (size_t i = 0; i < pool_size_; ++i) {
    Page *page = &pages_[i];
    if (page->page_id_!=INVALID_PAGE_ID && page->is_dirty_){
      disk_manager_->WritePage(page->page_id_,page->data_);
      page->is_dirty_ = false;
    }
  }
}

}  // namespace bustub
