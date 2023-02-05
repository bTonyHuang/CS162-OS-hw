/*
 * Implementation of the word_count interface using Pintos lists.
 *
 * You may modify this file, and are expected to modify it.
 */

/*
 * Copyright © 2021 University of California, Berkeley
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

#ifndef PINTOS_LIST
#error "PINTOS_LIST must be #define'd when compiling word_count_l.c"
#endif

#include "word_count.h"


void init_words(word_count_list_t* wclist) { /* TODO */
  list_init(wclist);
}

size_t len_words(word_count_list_t* wclist) {
  /* Return -1 if any errors are
     encountered in the body of
     this function.
  */
  /* TODO */
  if(!wclist)
    return -1;

  return list_size(wclist);
}

word_count_t* find_word(word_count_list_t* wclist, char* word) {
  /* TODO */
  /* Return count for word, if it exists */
  word_count_t* wc=NULL;
  //if wchead or word is NULL then return NULL
  if(!word||!wclist)
    return wc;
  
  struct list_elem *e;
  for(e=list_begin(wclist);e!=list_end(wclist);e=list_next(e)){
    //convert pointer
    word_count_t* wc_t = list_entry (e, struct word_count, elem);
    if(!strcmp(wc_t->word,word)){
      wc=wc_t;
      break;
    }
  }
  return wc;
}

char *new_string(char *str) {
  char *new_str = (char *) malloc(strlen(str) + 1);
  if (new_str == NULL) {
    return NULL;
  }
  return strcpy(new_str, str);
}

word_count_t* add_word(word_count_list_t* wclist, char* word) {
  /* TODO */
  /* If word is present in word_counts list, increment the count.
     Otherwise insert with count 1.
     Returns word_count_t* wc if no errors are encountered in the body of this function; 
     NULL otherwise.
  */
  //param validation
  if(!wclist||!word){
    printf("add word is NULL\n");
    return NULL;
  }

  word_count_t* wc=find_word(wclist,word);

  //presence, count++
  if(wc)
    wc->count++;
  //insert with count 1.
  else{
    wc=(word_count_t*)calloc(sizeof(word_count_t),sizeof(char));
    if(!wc){
      printf("calloc failed\n");
      return NULL;
    }

    wc->count=1;

    wc->word=new_string(word);
    if(!wc->word){
      printf("new_string failed\n");
      return NULL;
    }

    list_push_front(wclist,&wc->elem);
  }

  return wc;
}

void fprint_words(word_count_list_t* wclist, FILE* outfile) { /* TODO */
   /* print word counts to a file */
  //first: convert pointer
  struct list_elem *e;
  for(e=list_begin(wclist);e!=list_end(wclist);e=list_next(e)){
    word_count_t* wc = list_entry (e, struct word_count, elem);
    fprintf(outfile, "%i\t%s\n", wc->count, wc->word);
  }
  return;
}

static bool less_list(const struct list_elem* ewc1, const struct list_elem* ewc2, void* aux) {
  /* TODO */
  //first: convert pointer
  word_count_t* wc1 = list_entry (ewc1, struct word_count, elem);
  word_count_t* wc2 = list_entry (ewc2, struct word_count, elem);

   if(!wc1)
    return true;
  else if(!wc2)
    return false;
  else if(wc1->count==wc2->count)
    return strcmp(wc1->word,wc2->word)<0?true:false;
  else if(wc1->count<wc2->count)
    return true;
  else
    return false;
}

void wordcount_sort(word_count_list_t* wclist,
                    bool less(const word_count_t*, const word_count_t*)) {
  list_sort(wclist, less_list, less);
}
