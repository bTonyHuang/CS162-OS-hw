/*
 * mm_alloc.c
 */

#include "mm_alloc.h"

#include <stdlib.h>
#include <unistd.h>
#include <stdio.h>
#include <string.h>

struct metadata_alloc{
  void* prev;
  void* next;
  bool free;
  int size;
} Metadata;

// struct alloc_list_node{
//   //struct list_elem elem; /*list element*/
//   Metadata metadata;  /*block metadata*/
//   struct alloc_list_node* next_node;
// };

// typedef struct alloc_list_node* Alloc_list;



void* mm_malloc(size_t size) {

  return NULL;
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

  /*first free the block referenced by ptr, 
  then mm_malloc a block of the specified size, zero-fill the block, 
  and finally memcpy the old data to the new block. */
  mm_free(ptr);
  void* new_block = mm_malloc(size);
  /*Return NULL if you cannot allocate the new requested size. In this case, do not modify the original block*/
  if(!new_block)
    return NULL;

  const void* old_block = ptr;
  /*Make sure you handle the case where size is less than the original size.*/
  const int cpy_size = size;
  memset(new_block, 0, size);
  memcpy(new_block, old_block, cpy_size);

  return new_block;
}

void mm_free(void* ptr) {
  //do nothing if ptr is NULL
  if(!ptr)
    return;

  //coalesce consecutive free blocks upon freeing a block that is adjacent to other free block(s).

}
