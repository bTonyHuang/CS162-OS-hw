#include "userprog/syscall.h"
#include <stdio.h>
#include <string.h>
#include <round.h>
#include <syscall-nr.h>
#include "threads/interrupt.h"
#include "threads/thread.h"
#include "threads/vaddr.h"
#include "threads/palloc.h"
#include "pagedir.h"
#include "filesys/filesys.h"
#include "filesys/file.h"

static void syscall_handler(struct intr_frame*);

void syscall_init(void) { intr_register_int(0x30, 3, INTR_ON, syscall_handler, "syscall"); }

void syscall_exit(int status) {
  printf("%s: exit(%d)\n", thread_current()->name, status);
  thread_exit();
}

/*
 * This does not check that the buffer consists of only mapped pages; it merely
 * checks the buffer exists entirely below PHYS_BASE.
 */
static void validate_buffer_in_user_region(const void* buffer, size_t length) {
  uintptr_t delta = PHYS_BASE - buffer;
  if (!is_user_vaddr(buffer) || length > delta)
    syscall_exit(-1);
}

/*
 * This does not check that the string consists of only mapped pages; it merely
 * checks the string exists entirely below PHYS_BASE.
 */
static void validate_string_in_user_region(const char* string) {
  uintptr_t delta = PHYS_BASE - (const void*)string;
  if (!is_user_vaddr(string) || strnlen(string, delta) == delta)
    syscall_exit(-1);
}

static int syscall_open(const char* filename) {
  struct thread* t = thread_current();
  if (t->open_file != NULL)
    return -1;

  t->open_file = filesys_open(filename);
  if (t->open_file == NULL)
    return -1;

  return 2;
}

static int syscall_write(int fd, void* buffer, unsigned size) {
  struct thread* t = thread_current();
  if (fd == STDOUT_FILENO) {
    putbuf(buffer, size);
    return size;
  } else if (fd != 2 || t->open_file == NULL)
    return -1;

  return (int)file_write(t->open_file, buffer, size);
}

static int syscall_read(int fd, void* buffer, unsigned size) {
  struct thread* t = thread_current();
  if (fd != 2 || t->open_file == NULL)
    return -1;

  return (int)file_read(t->open_file, buffer, size);
}

static void syscall_close(int fd) {
  struct thread* t = thread_current();
  if (fd == 2 && t->open_file != NULL) {
    file_close(t->open_file);
    t->open_file = NULL;
  }
}

static void* syscall_sbrk(intptr_t increment) {
  struct thread* t = thread_current();
  const void* original_segment_break = (void*)t->segment_break;
  //arguments validation check
  if(!is_user_vaddr(t->segment_break+increment))
    return (void*)-1;
  else if(increment==0)
    return (void*)original_segment_break;

  //check if segment_break would be still in the same page
  t->segment_break += increment;
  if(t->segment_break<t->heap)
    return (void*)-1;
  bool cross_page_down_boundary =
      (uint8_t*)pg_round_down(original_segment_break) >= t->segment_break;
  bool cross_page_up_boundary = t->segment_break > (uint8_t*)pg_round_up(original_segment_break);

  //get a new page via palloc_get_page(), return (void*)-1 if failed
  bool success;
  if (cross_page_up_boundary) {
    int pg_cnt = (pg_round_up(t->segment_break) - pg_round_up(original_segment_break)) / PGSIZE;
    //dealing edge cases
    if(pg_cnt<=0){
      t->segment_break -= increment;
      return (void*)-1;
    }
    uint8_t* kpage;
    kpage = palloc_get_multiple(PAL_ZERO | PAL_USER, pg_cnt);
    success = !!kpage;
    for (int i = 0; i < pg_cnt; i++) {
      //map the new page using pagedir_set_page()
      if (kpage) {
        success =
            pagedir_get_page(t->pagedir,
                             (uint8_t*)pg_round_up(original_segment_break) + i * PGSIZE) == NULL &&
            pagedir_set_page(t->pagedir, (uint8_t*)pg_round_up(original_segment_break) + i * PGSIZE,
                             kpage + i * PGSIZE, true);
        if (!success) {
          palloc_free_multiple(kpage, i + 1);
        }
      }
      //undo the operations to retain t->segment_break
      if (!success) {
        t->segment_break -= increment;
        return (void*)-1;
      }
    }
  }

  //deallocate pages via palloc_free_page() and pagedir_clear_page()
  if(cross_page_down_boundary){
    int pg_cnt = (pg_round_up(original_segment_break) - pg_round_up(t->segment_break)) / PGSIZE;
    for (int i = 0; i < pg_cnt; i++) {
      uint8_t* kpage =
          pagedir_get_page(t->pagedir, pg_round_down(original_segment_break) - i * PGSIZE);
      if (kpage) {
        pagedir_clear_page(t->pagedir, pg_round_down(original_segment_break) - i * PGSIZE);
        palloc_free_page(kpage);
      }
    }
  }

  return (void*)original_segment_break;
}

static void syscall_handler(struct intr_frame* f) {
  uint32_t* args = (uint32_t*)f->esp;
  struct thread* t = thread_current();
  t->in_syscall = true;

  validate_buffer_in_user_region(args, sizeof(uint32_t));
  switch (args[0]) {
    case SYS_EXIT:
      validate_buffer_in_user_region(&args[1], sizeof(uint32_t));
      syscall_exit((int)args[1]);
      break;

    case SYS_OPEN:
      validate_buffer_in_user_region(&args[1], sizeof(uint32_t));
      validate_string_in_user_region((char*)args[1]);
      f->eax = (uint32_t)syscall_open((char*)args[1]);
      break;

    case SYS_WRITE:
      validate_buffer_in_user_region(&args[1], 3 * sizeof(uint32_t));
      validate_buffer_in_user_region((void*)args[2], (unsigned)args[3]);
      f->eax = (uint32_t)syscall_write((int)args[1], (void*)args[2], (unsigned)args[3]);
      break;

    case SYS_READ:
      validate_buffer_in_user_region(&args[1], 3 * sizeof(uint32_t));
      validate_buffer_in_user_region((void*)args[2], (unsigned)args[3]);
      f->eax = (uint32_t)syscall_read((int)args[1], (void*)args[2], (unsigned)args[3]);
      break;

    case SYS_CLOSE:
      validate_buffer_in_user_region(&args[1], sizeof(uint32_t));
      syscall_close((int)args[1]);
      break;

    case SYS_SBRK:
      validate_buffer_in_user_region(&args[1], sizeof(intptr_t));
      f->eax = (uint32_t)syscall_sbrk((intptr_t)args[1]);
      break;

    default:
      printf("Unimplemented system call: %d\n", (int)args[0]);
      break;
  }

  t->in_syscall = false;
}
