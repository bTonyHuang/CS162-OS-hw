/*
 * mm_alloc.c
 */

#include "mm_alloc.h"

#include <stdlib.h>
#include <unistd.h>
#include <stdio.h>
#include <string.h>

enum freeStatus { FREE, BUSY };
enum boolStatus { FALSE = 0, TRUE };

typedef struct metadata_alloc Metadata;

struct metadata_alloc{
  Metadata* prev;
  Metadata* next;
  __uint8_t status;
  size_t size;
};

Metadata* block_list = NULL;

static __uint8_t push_list_end(Metadata* head, Metadata* node){
  if(!head||!node)
    return FALSE;
  while (head->next) {
    head = head->next;
  }
  head->next = node;
  node->prev = head;
  return TRUE;
}

void* mm_malloc(size_t size) {
  //validation check
  if(size<=0)
    return NULL;

  size += sizeof(Metadata);

  //use first-fit algorithm to find suitable block
  if(block_list){
    Metadata* pointer = block_list;
    while (pointer){
      if(pointer->status == FREE && pointer->size >= size){
        //check if could divide a new block
        if(pointer->size-size > sizeof(Metadata)){
          Metadata* divide_block = (void*)pointer + size;

          //rebuild connections
          divide_block->prev = pointer;
          divide_block->next = pointer->next;
          if(pointer->next)
            pointer->next->prev = divide_block;
          pointer->next = divide_block;

          //set status
          divide_block->status = FREE;

          //reset size
          divide_block->size = pointer->size - size;
          pointer->size = size;
        }
        pointer->status = BUSY;
        //zero-fill the space
        memset((void*)(pointer + 1), 0, pointer->size - sizeof(Metadata));
        return (void*)(pointer + 1);
      }
      pointer = pointer->next;
    }//end of while loop
  }

  //no current free block fit, then call sbrk to apply for more heap memory
  Metadata* new_meta = sbrk(size);
  if(new_meta==(void*)-1)//sbrk fail
    return NULL;
  new_meta->size = size;
  new_meta->status = BUSY;
  new_meta->prev = new_meta->next = NULL;

  if (!block_list)
    block_list = new_meta;
  else if(!push_list_end(block_list,new_meta))
    return NULL;

  //zero-fill the space
  memset((void*)(new_meta + 1), 0, new_meta->size - sizeof(Metadata));
  return (void*)(new_meta + 1);
}

void* mm_realloc(void* ptr, size_t size) {
  //handling edge cases
  if(!ptr&&!size)//mm_realloc(NULL, 0) is equivalent to calling mm_malloc(0), which should just return NULL.
    return NULL;
  else if(!ptr)//mm_realloc(NULL, n) is equivalent to calling mm_malloc(n).
    return mm_malloc(size);
  else if(!size){
    mm_free(ptr); //mm_realloc(ptr, 0) is equivalent to calling mm_free(ptr) and returning NULL.
    return NULL;
  }

  /*first mm_malloc a block of the specified size, zero-fill the block, 
  and then memcpy the old data to the new block. */
  void* new_block = mm_malloc(size);
  /*Return NULL if you cannot allocate the new requested size. In this case, do not modify the original block*/
  if(!new_block)
    return NULL;

  /*Make sure you handle the case where size is less than the original size.*/
  const void* old_block = ptr;
  const Metadata* old_block_metadata = (Metadata*)(old_block - sizeof(Metadata));
  const size_t original_size = old_block_metadata->size;
  const size_t cpy_size = size < original_size ? size : original_size;
  memcpy(new_block, old_block, cpy_size);


  //finally free the old_block
  mm_free(ptr);
  return new_block;
}

void mm_free(void* ptr) {
  //do nothing if ptr is NULL
  if(!ptr)
    return;

  Metadata* ptr_metadata = (Metadata*)(ptr - sizeof(Metadata));

  //validity check
  Metadata* check_iter = block_list;
  __uint8_t compatible = FALSE;
  while (check_iter) {
    if(check_iter==ptr_metadata){
      compatible = TRUE;
      break;
    }
    check_iter = check_iter->next;
  }
  if(!compatible)
    return;

  ptr_metadata->status = FREE;
  //coalesce consecutive free blocks upon freeing a block that is adjacent to other free block(s).
  //combine prev
  size_t prev_size = 0;
  Metadata* prev_current;
  Metadata* prev_iter = ptr_metadata->prev;
  while (prev_iter) {
    if(prev_iter->status==BUSY)
      break;
    prev_size += prev_iter->size;
    prev_current = prev_iter;
    prev_iter = prev_iter->prev;
  }
  //combine next
  size_t next_size = 0;
  Metadata* next_current;
  Metadata* next_iter = ptr_metadata->next;
  while (next_iter) {
    if (next_iter->status == BUSY)
      break;
    next_size += next_iter->size;
    next_current = next_iter;
    next_iter = next_iter->next;
  }

  //no coalesce
  if(!prev_size&&!next_size)
    return;
  //coalesce next, retaining the ptr_metadata
  else if(!prev_size&&next_size){
    ptr_metadata->size += next_size;
    //rebuild connection
    ptr_metadata->next = next_current->next;
    if(ptr_metadata->next)
      ptr_metadata->next->prev = ptr_metadata;
  }
  //coalesce prev, retaining the ptr_metadata
  else {
    prev_current->size = ptr_metadata->size + prev_size;
    //rebuild connection
    if(!next_size)
      prev_current->next=ptr_metadata->next;
    //adding next_size
    else{
      prev_current->size += next_size;
      prev_current->next = next_current->next;
    }
    
    if (prev_current->next)
      prev_current->next->prev = prev_current;
  }
}
